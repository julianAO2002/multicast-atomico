# multicast-atomico

Comunicación en grupo sobre **IP multicast** con entrega **atómica** (todos o ninguno) y un
**nodo coordinador** que recibe los mensajes de computadoras que están fuera del grupo.

Está escrito en Python 3.12 y usa solo la biblioteca estándar (`socket`, `threading`, `json`).
Con Docker Compose cada nodo corre en su propio contenedor, con IP propia, así que cada
contenedor hace de una computadora distinta de la misma red local.

```
                     red local (bridge "lan")
   ┌─────────┐  DATOS / CONFIRMAR / ABORTAR  →  239.0.0.1:5007
   │  nodo2  │──────────────┬──────────────┬──────────────┐
   └─────────┘              ▼              ▼              ▼
        ▲              ┌─────────┐    ┌─────────┐    ┌─────────┐
        └─ ACK/LISTO ──│  nodo1  │    │  nodo3  │    │ (nodo2) │
          (unicast)    │ coord.  │    └─────────┘    └─────────┘
                       └────▲────┘
                            │ TCP 7000: "texto\n"  →  "OK nodo1:1" | "ABORTADO"
                       ┌────┴────┐
                       │ externo │  4ª computadora, fuera del grupo
                       └─────────┘
```

## Cómo funciona

### Grupo

- Todos los miembros se unen a la misma dirección de grupo, `239.0.0.1:5007` (rango
  multicast de alcance administrativo), con `IP_ADD_MEMBERSHIP`. El kernel avisa por IGMP y a
  partir de ahí acepta los datagramas enviados a esa dirección.
- Un solo `sendto` a la dirección de grupo le llega a todos los miembros: la red hace la copia.
- Los datagramas se envían con TTL 1, así que no salen de la red local.
- El grupo es **cerrado** y de membresía **estática**: cada nodo conoce la lista `MIEMBROS`. Si
  llegan DATOS de alguien que no está en la lista, se descartan y se registra el rechazo.

### Entrega atómica (compromiso en dos fases)

Cada mensaje tiene un id `origen:secuencia`. El emisor:

1. Envía `DATOS` al grupo. Cada miembro lo guarda como **pendiente** (todavía no lo entrega) y
   responde `ACK` por unicast al socket del emisor.
2. Si en `timeout` no tiene el ACK de todos, retransmite `DATOS` al grupo, hasta `reintentos` veces.
3. Si respondieron **todos**, lo entrega él mismo y envía `CONFIRMAR`. Cada miembro entrega el
   pendiente y responde `LISTO`.
4. Si **alguno** no respondió, envía `ABORTAR`: cada miembro descarta el pendiente y responde
   `LISTO`. Nadie lo entrega.

Las retransmisiones generan duplicados. Cada nodo recuerda los ids ya entregados o abortados,
así que un mensaje nunca se entrega dos veces. Un nodo envía de a un mensaje por vez, por eso
cada miembro recibe los mensajes de un mismo emisor en el orden en que se enviaron (orden FIFO
por emisor).

| Mensaje     | Sentido              | Transporte | Efecto en el receptor                    |
|-------------|----------------------|------------|------------------------------------------|
| `DATOS`     | emisor → grupo       | multicast  | lo guarda como pendiente y responde `ACK` |
| `ACK`       | miembro → emisor     | unicast    | cuenta al miembro como "lo tiene"         |
| `CONFIRMAR` | emisor → grupo       | multicast  | entrega el pendiente y responde `LISTO`   |
| `ABORTAR`   | emisor → grupo       | multicast  | descarta el pendiente y responde `LISTO`  |
| `LISTO`     | miembro → emisor     | unicast    | cuenta al miembro como "aplicó la decisión" |

Los mensajes son JSON en UTF-8 y cada uno viaja en un único datagrama UDP.

### Coordinador y mensajes externos

El nodo que arranca con `PUERTO_COORDINADOR` escucha conexiones TCP. Una computadora externa
le manda una línea por mensaje. El coordinador la difunde al grupo con el mismo protocolo
atómico y le contesta `OK <id>` si se entregó a todos, o `ABORTADO` si no se pudo. Así, quien
está fuera del grupo no necesita conocer a los miembros ni la dirección multicast.

### Limitaciones

- Si el emisor se cae entre la fase de `ACK` y la de decisión, los miembros quedan con el
  mensaje pendiente (es el bloqueo clásico del compromiso en dos fases).
- El orden es FIFO por emisor: mensajes de emisores distintos pueden entregarse en distinto
  orden en cada nodo.
- El control de grupo cerrado mira el nombre del emisor. No hay autenticación.

## Estructura

```
grupo/
├── protocolo.py   mensajes (JSON) y sockets: unión al grupo, TTL, interfaz multicast
├── nodo.py        miembro del grupo: envío atómico, recepción, coordinador TCP
└── externo.py     computadora externa: envía al coordinador (y prueba un envío directo)
tests/test_grupo.py  pruebas automáticas (unittest) con varios nodos en la misma máquina
Dockerfile, docker-compose.yml
```

## Uso con Docker

Hace falta Docker con Compose v2.

```bash
# nodo1 (coordinador), nodo2 y nodo3 forman el grupo; nodo2 difunde dos mensajes
# y "externo" manda dos más a través del coordinador (y un envío directo que se rechaza).
docker compose up --build --abort-on-container-exit --exit-code-from externo

# Falla: nodo3 descarta todo lo que envía, nunca llega su ACK y todos los envíos se abortan.
PERDIDA_NODO3=1 docker compose up --build --abort-on-container-exit --exit-code-from externo

# Pruebas automáticas dentro de un contenedor
docker compose --profile tests run --rm --build tests
```

En PowerShell, la variable se define antes con `$env:PERDIDA_NODO3="1"`.

Para escribir mensajes a mano desde un nodo: `docker compose up -d` y luego
`docker attach multicast-atomico-nodo2-1`. Cada línea que escribas se difunde al grupo.
Para salir sin detener el contenedor, usá Ctrl+P Ctrl+Q.

Salida de ejemplo:

```
nodo2-1    | [nodo2] ENTREGADO nodo2:1 de nodo2: Hola grupo, soy nodo2
nodo1-1    | [nodo1] ENTREGADO nodo2:1 de nodo2: Hola grupo, soy nodo2
nodo3-1    | [nodo3] ENTREGADO nodo2:1 de nodo2: Hola grupo, soy nodo2
externo-1  | [externo] envío directo al grupo sin pasar por el coordinador
nodo1-1    | [nodo1] rechazado intruso:1: 'intruso' no es miembro del grupo (grupo cerrado)
nodo1-1    | [nodo1] conexión externa desde 172.23.0.5:39510
nodo2-1    | [nodo2] ENTREGADO nodo1:1 de nodo1 (externo 172.23.0.5:39510): Mensaje desde fuera del grupo
externo-1  | [externo] 'Mensaje desde fuera del grupo' -> OK nodo1:1
```

## Uso sin Docker

En una sola máquina, por la interfaz de loopback. Cada nodo usa un puerto unicast distinto:

```bash
python -m grupo.nodo --nombre nodo1 --interfaz 127.0.0.1 --puerto 6001 --coordinador 7000
python -m grupo.nodo --nombre nodo2 --interfaz 127.0.0.1 --puerto 6002
python -m grupo.nodo --nombre nodo3 --interfaz 127.0.0.1 --puerto 6003 --mensajes "hola|42"
python -m grupo.externo --coordinador 127.0.0.1:7000 --mensajes "desde afuera"
```

Entre computadoras reales de la misma LAN, cada una corre su nodo con la interfaz por defecto
(`0.0.0.0`), y el externo apunta a `IP_del_coordinador:7000`. Hay que permitir en el firewall
UDP 5007 (grupo), UDP 6000 (ACK/LISTO) y TCP 7000 (coordinador).

### Opciones

| Opción (variable)                     | Por defecto          | Descripción                                   |
|---------------------------------------|----------------------|-----------------------------------------------|
| `--nombre` (`NOMBRE`)                 | hostname             | nombre del nodo; debe estar en `--miembros`   |
| `--miembros` (`MIEMBROS`)             | `nodo1,nodo2,nodo3`  | miembros del grupo cerrado                    |
| `--grupo` (`GRUPO`)                   | `239.0.0.1:5007`     | dirección y puerto multicast                  |
| `--interfaz` (`INTERFAZ`)             | `0.0.0.0`            | interfaz para unirse y enviar al grupo        |
| `--puerto` (`PUERTO`)                 | `6000`               | puerto UDP propio para `ACK`/`LISTO`          |
| `--coordinador` (`PUERTO_COORDINADOR`)| —                    | si se indica, el nodo acepta externos por TCP |
| `--mensajes` (`MENSAJES`)             | —                    | mensajes a difundir al iniciar, separados por `\|` |
| `--espera` (`ESPERA`)                 | `2`                  | segundos antes de enviar                      |
| `--perdida` (`PERDIDA`)               | `0`                  | fracción de datagramas salientes descartados  |
| `--duracion` (`DURACION`)             | `0`                  | segundos de vida del nodo (0 = sin límite)    |

`grupo.externo` acepta `--coordinador` (`COORDINADOR`), `--mensajes`, `--espera` e `--intruso`
(`INTRUSO=1`). Termina con código 0 si todos sus mensajes se entregaron.

## Pruebas

```bash
python -m unittest discover -s tests -v
```

Las pruebas cubren: la entrega a los tres miembros; la entrega única con 30 % de pérdida; el
aborto cuando un miembro no está o no responde (nadie entrega); el envío externo por el
coordinador con su respuesta `OK`/`ABORTADO`; y el rechazo de un emisor que no es miembro.
