"""Computadora externa al grupo: envía mensajes al grupo a través del coordinador.

Se conecta por TCP al nodo coordinador, le manda una línea por mensaje y espera la
respuesta: "OK <id>" si el grupo entero lo entregó o "ABORTADO" si no.

Con --intruso además intenta saltear al coordinador mandando DATOS directo a la
dirección multicast; como el grupo es cerrado, los miembros lo rechazan.

    python -m grupo.externo --coordinador nodo1:7000 --mensajes "hola grupo|42"
"""

import argparse
import os
import socket
import sys
import time

from grupo.nodo import GRUPO_POR_DEFECTO
from grupo.protocolo import DATOS, codificar, parsear_direccion, socket_unicast


def conectar(direccion, intentos=20, pausa=0.5):
    """Conecta al coordinador reintentando mientras arranca."""
    for intento in range(intentos):
        try:
            return socket.create_connection(direccion, timeout=30)
        except OSError:
            if intento == intentos - 1:
                raise
            time.sleep(pausa)


def enviar_por_coordinador(direccion, mensajes):
    """Manda cada mensaje al coordinador y devuelve sus respuestas en orden."""
    respuestas = []
    with conectar(direccion) as conexion, conexion.makefile("r", encoding="utf-8") as entrada:
        for texto in mensajes:
            conexion.sendall(f"{texto}\n".encode("utf-8"))
            respuestas.append(entrada.readline().strip())
    return respuestas


def enviar_como_intruso(grupo, interfaz, texto):
    with socket_unicast(interfaz) as sock:
        sock.sendto(codificar(DATOS, id="intruso:1", origen="intruso", datos=texto), grupo)


def main(argv=None):
    entorno = os.environ.get
    parser = argparse.ArgumentParser(description="Emisor externo al grupo")
    parser.add_argument("--coordinador", default=entorno("COORDINADOR", "nodo1:7000"), help="host:puerto TCP")
    parser.add_argument("--mensajes", default=entorno("MENSAJES", "hola grupo"), help="mensajes separados por |")
    parser.add_argument("--espera", type=float, default=float(entorno("ESPERA", "0")))
    parser.add_argument("--intruso", action="store_true", default=entorno("INTRUSO", "0") == "1",
                        help="intentar también un envío multicast directo (debe ser rechazado)")
    parser.add_argument("--grupo", default=entorno("GRUPO", GRUPO_POR_DEFECTO))
    parser.add_argument("--interfaz", default=entorno("INTERFAZ", "0.0.0.0"))
    args = parser.parse_args(argv)

    time.sleep(args.espera)
    if args.intruso:
        print("[externo] envío directo al grupo sin pasar por el coordinador", flush=True)
        enviar_como_intruso(parsear_direccion(args.grupo), args.interfaz, "mensaje de un intruso")

    mensajes = [m for m in args.mensajes.split("|") if m]
    direccion = parsear_direccion(args.coordinador)
    todos_ok = True
    for texto, respuesta in zip(mensajes, enviar_por_coordinador(direccion, mensajes)):
        print(f"[externo] {texto!r} -> {respuesta}", flush=True)
        todos_ok &= respuesta.startswith("OK")
    return 0 if todos_ok else 1


if __name__ == "__main__":
    sys.exit(main())
