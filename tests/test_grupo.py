"""Pruebas del multicast atómico con varios nodos en la misma máquina.

Cada prueba usa un puerto de grupo distinto para no recibir tráfico de otra.
Por defecto el multicast viaja por la interfaz de loopback (INTERFAZ_PRUEBAS lo cambia).

    python -m unittest discover -s tests -v
"""

import os
import socket
import sys
import time
import unittest

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, RAIZ)

from grupo.externo import enviar_como_intruso, enviar_por_coordinador  # noqa: E402
from grupo.nodo import Nodo  # noqa: E402
from grupo.protocolo import (ACK, DATOS, codificar, decodificar, parsear_direccion,  # noqa: E402
                             socket_unicast)

INTERFAZ = os.environ.get("INTERFAZ_PRUEBAS", "127.0.0.1")
GRUPO_IP = "239.0.0.1"
MIEMBROS = ["nodo1", "nodo2", "nodo3"]


def puerto_libre(tipo=socket.SOCK_DGRAM):
    with socket.socket(socket.AF_INET, tipo) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def esperar(condicion, limite=5):
    fin = time.monotonic() + limite
    while time.monotonic() < fin:
        if condicion():
            return True
        time.sleep(0.05)
    return condicion()


class GrupoBase(unittest.TestCase):
    def setUp(self):
        self.grupo = (GRUPO_IP, puerto_libre())
        self.nodos = {}

    def tearDown(self):
        for nodo in self.nodos.values():
            nodo.cerrar()

    def crear(self, nombre, **opciones):
        opciones.setdefault("timeout", 0.1)
        opciones.setdefault("log", lambda _texto: None)
        nodo = Nodo(nombre, MIEMBROS, grupo=self.grupo, interfaz=INTERFAZ, **opciones)
        self.nodos[nombre] = nodo
        return nodo

    def datos_entregados(self, nombre):
        return [datos for _id, _origen, datos, _externo in self.nodos[nombre].entregas]


class Protocolo(unittest.TestCase):
    def test_codificar_y_decodificar(self):
        mensaje = decodificar(codificar(DATOS, id="nodo1:1", origen="nodo1", datos="ñandú 42"))
        self.assertEqual(mensaje, {"tipo": DATOS, "id": "nodo1:1", "origen": "nodo1", "datos": "ñandú 42"})

    def test_descarta_datagramas_ajenos(self):
        self.assertIsNone(decodificar(b"\xff\xfe basura"))
        self.assertIsNone(decodificar(b'{"tipo": "OTRO", "id": "x"}'))
        self.assertIsNone(decodificar(b'["DATOS"]'))

    def test_parsear_direccion(self):
        self.assertEqual(parsear_direccion("239.0.0.1:5007"), ("239.0.0.1", 5007))
        self.assertEqual(parsear_direccion("nodo1", 7000), ("nodo1", 7000))


class EntregaAtomica(GrupoBase):
    def test_los_tres_miembros_entregan_el_mensaje(self):
        for nombre in MIEMBROS:
            self.crear(nombre)
        id_msg = self.nodos["nodo1"].enviar("42")
        self.assertEqual(id_msg, "nodo1:1")
        for nombre in MIEMBROS:
            self.assertTrue(esperar(lambda: self.datos_entregados(nombre) == ["42"]), nombre)

    def test_con_perdida_cada_miembro_entrega_una_sola_vez(self):
        for i, nombre in enumerate(MIEMBROS):
            self.crear(nombre, perdida=0.3, semilla=i, reintentos=30)
        enviados = [f"mensaje {i}" for i in range(5)]
        for texto in enviados:
            self.assertIsNotNone(self.nodos["nodo2"].enviar(texto))
        for nombre in MIEMBROS:
            self.assertTrue(esperar(lambda: self.datos_entregados(nombre) == enviados), nombre)

    def test_si_un_miembro_no_responde_nadie_lo_entrega(self):
        # nodo3 figura como miembro pero no está corriendo: no puede haber ACK de todos.
        self.crear("nodo1", reintentos=3)
        self.crear("nodo2", reintentos=3)
        self.assertIsNone(self.nodos["nodo1"].enviar("no debe llegar"))
        time.sleep(0.3)
        self.assertEqual(self.datos_entregados("nodo1"), [])
        self.assertEqual(self.datos_entregados("nodo2"), [])
        self.assertEqual(self.nodos["nodo2"]._pendientes, {})

    def test_miembro_que_descarta_todo_provoca_aborto(self):
        self.crear("nodo1", reintentos=3)
        self.crear("nodo2", reintentos=3)
        self.crear("nodo3", perdida=1.0)  # recibe pero sus ACK nunca salen
        self.assertIsNone(self.nodos["nodo1"].enviar("no debe llegar"))
        time.sleep(0.3)
        for nombre in MIEMBROS:
            self.assertEqual(self.datos_entregados(nombre), [], nombre)


class GrupoCerradoYCoordinador(GrupoBase):
    def test_mensaje_externo_pasa_por_el_coordinador(self):
        coordinador = self.crear("nodo1", puerto_coordinador=0)
        self.crear("nodo2")
        self.crear("nodo3")
        direccion = ("127.0.0.1", coordinador.direccion_coordinador[1])
        respuestas = enviar_por_coordinador(direccion, ["hola grupo", "7"])
        self.assertEqual(respuestas, ["OK nodo1:1", "OK nodo1:2"])
        for nombre in MIEMBROS:
            self.assertTrue(esperar(lambda: self.datos_entregados(nombre) == ["hola grupo", "7"]), nombre)
            self.assertTrue(all(externo for *_resto, externo in self.nodos[nombre].entregas))

    def test_coordinador_avisa_si_el_grupo_aborta(self):
        coordinador = self.crear("nodo1", puerto_coordinador=0, reintentos=2)
        self.crear("nodo2", reintentos=2)
        direccion = ("127.0.0.1", coordinador.direccion_coordinador[1])
        self.assertEqual(enviar_por_coordinador(direccion, ["sin nodo3"]), ["ABORTADO"])

    def test_envio_directo_de_un_no_miembro_es_rechazado(self):
        for nombre in MIEMBROS:
            self.crear(nombre)
        with socket_unicast(INTERFAZ) as escucha:
            escucha.settimeout(0.5)
            escucha.sendto(codificar(DATOS, id="intruso:1", origen="intruso", datos="x"), self.grupo)
            with self.assertRaises(socket.timeout):
                while True:  # ningún miembro debe contestar ACK al intruso
                    mensaje = decodificar(escucha.recvfrom(65507)[0])
                    if mensaje and mensaje["tipo"] == ACK:
                        break
        enviar_como_intruso(self.grupo, INTERFAZ, "otro intento")
        time.sleep(0.3)
        for nombre in MIEMBROS:
            self.assertEqual(self.datos_entregados(nombre), [], nombre)

    def test_nombre_fuera_de_la_lista_de_miembros(self):
        with self.assertRaises(ValueError):
            Nodo("nodo9", MIEMBROS, grupo=self.grupo, interfaz=INTERFAZ)


if __name__ == "__main__":
    unittest.main()
