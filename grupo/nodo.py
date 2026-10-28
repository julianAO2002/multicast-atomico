"""Nodo de un grupo cerrado con multicast atómico y, opcionalmente, rol de coordinador.

Envío atómico (compromiso en dos fases sobre IP multicast):

  1. El emisor manda DATOS al grupo y espera un ACK de cada miembro, retransmitiendo
     al grupo ante cada timeout.
  2a. Si respondieron todos, manda CONFIRMAR: cada miembro entrega el mensaje.
  2b. Si alguno no respondió tras los reintentos, manda ABORTAR: nadie lo entrega.

Así el mensaje llega a todos los miembros o a ninguno. Los duplicados (por
retransmisión) se detectan con el id del mensaje y se entregan una sola vez.

El grupo es cerrado: los DATOS de quien no figura en la lista de miembros se ignoran.
Una computadora externa manda sus mensajes por TCP al nodo coordinador, que los
difunde al grupo con el mismo protocolo atómico y le contesta el resultado.

    python -m grupo.nodo --nombre nodo1 --miembros nodo1,nodo2,nodo3 --coordinador 7000
"""

import argparse
import os
import random
import signal
import socket
import sys
import threading
import time

from grupo.protocolo import (ABORTAR, ACK, CONFIRMAR, DATOS, LISTO, TAM_MAX_DATAGRAMA, codificar,
                             decodificar, parsear_direccion, socket_grupo, socket_unicast)

GRUPO_POR_DEFECTO = "239.0.0.1:5007"


class Nodo:
    def __init__(self, nombre, miembros, grupo=("239.0.0.1", 5007), interfaz="0.0.0.0",
                 puerto_unicast=0, puerto_coordinador=None, perdida=0.0, timeout=0.3,
                 reintentos=10, semilla=None, entregar=None, log=None):
        if nombre not in miembros:
            raise ValueError(f"{nombre} no está en la lista de miembros {miembros}")
        self.nombre = nombre
        self.miembros = list(miembros)
        self.grupo = grupo
        self.perdida = perdida
        self.timeout = timeout
        self.reintentos = reintentos
        self.entregas = []                 # (id, origen, datos, externo) en orden de entrega
        self._al_entregar = entregar
        self._log = log or (lambda texto: print(f"[{self.nombre}] {texto}", flush=True))
        self._azar = random.Random(semilla)

        self._activo = True
        self._estado = threading.Condition()
        self._pendientes = {}              # id -> DATOS recibido, esperando la decisión
        self._entregados = set()
        self._abortados = set()
        self._rechazados = set()
        self._respuestas = {}              # (id, ACK|LISTO) -> nodos que respondieron
        self._envio = threading.Lock()     # un envío atómico a la vez por nodo
        self._secuencia = 0

        self._sock_grupo = socket_grupo(grupo[0], grupo[1], interfaz)
        self._sock_unicast = socket_unicast(interfaz, puerto_unicast)
        self.direccion = self._sock_unicast.getsockname()
        self._hilos = [threading.Thread(target=self._escuchar, args=(self._sock_grupo, self._procesar_grupo)),
                       threading.Thread(target=self._escuchar, args=(self._sock_unicast, self._procesar_respuesta))]

        self._servidor = None
        if puerto_coordinador is not None:
            self._servidor = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self._servidor.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self._servidor.bind(("" if interfaz == "0.0.0.0" else interfaz, puerto_coordinador))
            self._servidor.listen()
            self.direccion_coordinador = self._servidor.getsockname()
            self._hilos.append(threading.Thread(target=self._atender_externos))

        for hilo in self._hilos:
            hilo.daemon = True
            hilo.start()
        rol = f", coordinador en TCP {self.direccion_coordinador[1]}" if self._servidor else ""
        self._log(f"unido al grupo {grupo[0]}:{grupo[1]} (miembros: {', '.join(self.miembros)}{rol})")

    # ------------------------------------------------------------------ envío atómico

    def enviar(self, datos, externo=None):
        """Difunde `datos` al grupo. Devuelve el id si se entregó a todos, None si se abortó."""
        with self._envio:
            self._secuencia += 1
            id_msg = f"{self.nombre}:{self._secuencia}"
            paquete = codificar(DATOS, id=id_msg, origen=self.nombre, datos=datos, externo=externo)
            if len(paquete) > TAM_MAX_DATAGRAMA:
                raise ValueError("el mensaje no entra en un datagrama UDP")
            otros = set(self.miembros) - {self.nombre}

            if not self._fase(paquete, id_msg, ACK, otros):
                faltan = otros - self._respuestas.get((id_msg, ACK), set())
                self._log(f"ABORTADO {id_msg}: sin ACK de {', '.join(sorted(faltan))}")
                self._fase(codificar(ABORTAR, id=id_msg, origen=self.nombre), id_msg, LISTO, otros)
                return None

            # Todos lo guardaron: la decisión es entregar, empezando por el propio emisor.
            self._entregar(id_msg, self.nombre, datos, externo)
            if not self._fase(codificar(CONFIRMAR, id=id_msg, origen=self.nombre), id_msg, LISTO, otros):
                faltan = otros - self._respuestas.get((id_msg, LISTO), set())
                self._log(f"aviso: {', '.join(sorted(faltan))} no confirmó la entrega de {id_msg}")
            return id_msg

    def _fase(self, paquete, id_msg, esperado, nodos):
        """Manda `paquete` al grupo hasta que todos los `nodos` respondan `esperado`."""
        clave = (id_msg, esperado)
        for _ in range(1 + self.reintentos):
            self._enviar(paquete, self.grupo)
            with self._estado:
                if self._estado.wait_for(lambda: nodos <= self._respuestas.get(clave, set()), self.timeout):
                    return True
        return False

    def _enviar(self, paquete, destino):
        if self._azar.random() < self.perdida:
            return  # pérdida simulada del datagrama
        try:
            self._sock_unicast.sendto(paquete, destino)
        except OSError:
            pass  # socket cerrado o red caída: el protocolo lo trata como pérdida

    # ------------------------------------------------------------------ recepción

    def _escuchar(self, sock, procesar):
        sock.settimeout(0.2)
        while self._activo:
            try:
                paquete, origen = sock.recvfrom(TAM_MAX_DATAGRAMA)
            except socket.timeout:
                continue
            except OSError:
                break
            mensaje = decodificar(paquete)
            if mensaje is not None:
                procesar(mensaje, origen)

    def _procesar_grupo(self, mensaje, origen):
        emisor = mensaje.get("origen")
        if emisor == self.nombre:
            return  # copia propia devuelta por IP_MULTICAST_LOOP
        id_msg = mensaje["id"]
        if emisor not in self.miembros:
            if id_msg not in self._rechazados:
                self._rechazados.add(id_msg)
                self._log(f"rechazado {id_msg}: {emisor!r} no es miembro del grupo (grupo cerrado)")
            return

        tipo = mensaje["tipo"]
        with self._estado:
            if tipo == DATOS:
                if id_msg not in self._entregados and id_msg not in self._abortados:
                    self._pendientes[id_msg] = mensaje
                respuesta = ACK
            elif tipo == CONFIRMAR:
                pendiente = self._pendientes.pop(id_msg, None)
                if pendiente is None and id_msg not in self._entregados:
                    return  # nunca vimos los DATOS: no respondemos y el emisor reintenta
                respuesta = LISTO
            elif tipo == ABORTAR:
                if self._pendientes.pop(id_msg, None) is not None:
                    self._log(f"descartado {id_msg} (abortado por {emisor})")
                self._abortados.add(id_msg)
                respuesta = LISTO
            else:
                return
        if tipo == CONFIRMAR and pendiente is not None:
            self._entregar(id_msg, emisor, pendiente["datos"], pendiente.get("externo"))
        self._enviar(codificar(respuesta, id=id_msg, nodo=self.nombre), origen)

    def _procesar_respuesta(self, mensaje, _origen):
        if mensaje["tipo"] not in (ACK, LISTO) or mensaje.get("nodo") not in self.miembros:
            return
        with self._estado:
            self._respuestas.setdefault((mensaje["id"], mensaje["tipo"]), set()).add(mensaje["nodo"])
            self._estado.notify_all()

    def _entregar(self, id_msg, origen, datos, externo):
        with self._estado:
            if id_msg in self._entregados:
                return
            self._entregados.add(id_msg)
            self.entregas.append((id_msg, origen, datos, externo))
        via = f" (externo {externo})" if externo else ""
        self._log(f"ENTREGADO {id_msg} de {origen}{via}: {datos}")
        if self._al_entregar:
            self._al_entregar(id_msg, origen, datos, externo)

    # ------------------------------------------------------------------ coordinador

    def _atender_externos(self):
        self._servidor.settimeout(0.2)
        while self._activo:
            try:
                conexion, direccion = self._servidor.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            threading.Thread(target=self._atender_cliente, args=(conexion, direccion), daemon=True).start()

    def _atender_cliente(self, conexion, direccion):
        externo = f"{direccion[0]}:{direccion[1]}"
        self._log(f"conexión externa desde {externo}")
        with conexion, conexion.makefile("r", encoding="utf-8") as entrada:
            for linea in entrada:
                datos = linea.rstrip("\r\n")
                if not datos:
                    continue
                id_msg = self.enviar(datos, externo=externo)
                respuesta = f"OK {id_msg}\n" if id_msg else "ABORTADO\n"
                conexion.sendall(respuesta.encode("utf-8"))

    def cerrar(self):
        self._activo = False
        for sock in (self._sock_grupo, self._sock_unicast, self._servidor):
            if sock is not None:
                sock.close()
        for hilo in self._hilos:
            hilo.join(timeout=1)


def _interrumpir(_senal, _marco):
    raise KeyboardInterrupt


def main(argv=None):
    entorno = os.environ.get
    parser = argparse.ArgumentParser(description="Miembro de un grupo con multicast atómico")
    parser.add_argument("--nombre", default=entorno("NOMBRE", socket.gethostname()))
    parser.add_argument("--miembros", default=entorno("MIEMBROS", "nodo1,nodo2,nodo3"),
                        help="nombres de los miembros separados por coma")
    parser.add_argument("--grupo", default=entorno("GRUPO", GRUPO_POR_DEFECTO), help="IP:puerto multicast")
    parser.add_argument("--interfaz", default=entorno("INTERFAZ", "0.0.0.0"))
    parser.add_argument("--puerto", type=int, default=int(entorno("PUERTO", "6000")),
                        help="puerto UDP propio para ACK y LISTO")
    parser.add_argument("--coordinador", type=int, default=entorno("PUERTO_COORDINADOR"),
                        help="si se indica, el nodo es coordinador y acepta mensajes externos por TCP")
    parser.add_argument("--mensajes", default=entorno("MENSAJES", ""), help="mensajes a difundir, separados por |")
    parser.add_argument("--espera", type=float, default=float(entorno("ESPERA", "2")),
                        help="segundos antes de enviar (para que el resto se una al grupo)")
    parser.add_argument("--perdida", type=float, default=float(entorno("PERDIDA", "0")),
                        help="fracción de datagramas salientes que se descartan (simulación)")
    parser.add_argument("--duracion", type=float, default=float(entorno("DURACION", "0")),
                        help="segundos que vive el nodo; 0 = hasta que lo detengan")
    args = parser.parse_args(argv)

    nodo = Nodo(args.nombre, [m.strip() for m in args.miembros.split(",") if m.strip()],
                grupo=parsear_direccion(args.grupo), interfaz=args.interfaz, puerto_unicast=args.puerto,
                puerto_coordinador=args.coordinador, perdida=args.perdida)
    # docker stop manda SIGTERM: salir igual que con Ctrl+C (como PID 1 no hay manejador por defecto).
    signal.signal(signal.SIGTERM, _interrumpir)
    inicio = time.monotonic()
    try:
        time.sleep(args.espera)
        for texto in filter(None, args.mensajes.split("|")):
            nodo.enviar(texto)
        if sys.stdin.isatty():  # con terminal, cada línea escrita se difunde al grupo
            for linea in sys.stdin:
                if linea.strip():
                    nodo.enviar(linea.strip())
        while args.duracion <= 0 or time.monotonic() - inicio < args.duracion:
            time.sleep(0.2)
    except KeyboardInterrupt:
        pass
    finally:
        nodo.cerrar()


if __name__ == "__main__":
    main()
