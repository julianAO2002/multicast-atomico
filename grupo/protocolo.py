"""Mensajes del protocolo de multicast atómico y creación de sockets.

Cada mensaje es un objeto JSON en UTF-8 que viaja en un único datagrama UDP:

    DATOS      emisor -> grupo    propone un mensaje (queda pendiente en cada miembro)
    ACK        miembro -> emisor  "lo tengo guardado"
    CONFIRMAR  emisor -> grupo    todos lo tienen: entregarlo
    ABORTAR    emisor -> grupo    alguno no respondió: descartarlo
    LISTO      miembro -> emisor  "apliqué la decisión" (CONFIRMAR o ABORTAR)
"""

import json
import socket
import struct

DATOS = "DATOS"
ACK = "ACK"
CONFIRMAR = "CONFIRMAR"
ABORTAR = "ABORTAR"
LISTO = "LISTO"
TIPOS = {DATOS, ACK, CONFIRMAR, ABORTAR, LISTO}

# Un datagrama UDP sobre IPv4 lleva como máximo 65507 bytes de carga.
TAM_MAX_DATAGRAMA = 65507


def codificar(tipo, **campos):
    return json.dumps({"tipo": tipo, **campos}, ensure_ascii=False).encode("utf-8")


def decodificar(paquete):
    """Devuelve el mensaje como dict, o None si el datagrama no es del protocolo."""
    try:
        mensaje = json.loads(paquete.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None
    if not isinstance(mensaje, dict) or mensaje.get("tipo") not in TIPOS or "id" not in mensaje:
        return None
    return mensaje


def parsear_direccion(texto, puerto_por_defecto=None):
    """'host:puerto' -> (host, puerto)."""
    host, _, puerto = texto.rpartition(":")
    if not host:
        if puerto_por_defecto is None:
            raise ValueError(f"falta el puerto en {texto!r}")
        return puerto, puerto_por_defecto
    return host, int(puerto)


def socket_grupo(grupo, puerto, interfaz="0.0.0.0"):
    """Socket que recibe lo enviado a la dirección de grupo (IP multicast clase D)."""
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    # Varios procesos de la misma máquina pueden escuchar el mismo grupo y puerto.
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind(("", puerto))
    # IP_ADD_MEMBERSHIP: el kernel envía un IGMP Membership Report y empieza a aceptar el grupo.
    membresia = struct.pack("4s4s", socket.inet_aton(grupo), socket.inet_aton(interfaz))
    s.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, membresia)
    return s


def socket_unicast(interfaz="0.0.0.0", puerto=0, ttl=1):
    """Socket propio del nodo: envía al grupo y recibe las respuestas punto a punto.

    Como los DATOS salen desde este socket, los miembros responden ACK/LISTO a la
    dirección de origen del datagrama, sin necesidad de conocer al emisor de antemano.
    """
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    s.bind(("" if interfaz == "0.0.0.0" else interfaz, puerto))
    # TTL 1: los datagramas multicast no salen de la red local.
    s.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, ttl)
    s.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_LOOP, 1)
    s.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_IF, socket.inet_aton(interfaz))
    return s
