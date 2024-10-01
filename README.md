# Sistema de archivo musical de HMV

## Resumen:

  El objetivo es diseñar un sistema de almacenamiento de datos para HMV.music, una plataforma centrada en grabaciones musicales. El sistema debe soportar diversas actividades relacionadas con el almacenamiento de información sobre artistas, grabaciones, álbumes y conciertos.

## Características clave:

### Datos de Grabaciones Musicales:

  - Almacenar información sobre grabaciones realizadas tanto por solistas como por grupos (bandas, orquestas).
  - Las grabaciones se identifican mediante un código ISRC universal, y las composiciones se identifican con un código ISWC.
  - Se registran datos como las fechas de inicio y fin de la grabación, así como la duración total de la misma.

### Datos de Artistas:

  - Almacenar el nombre del artista (o del grupo), sus roles (por ejemplo, cantante, guitarrista) y el historial de su participación en diferentes formaciones.
  - Gestionar información sobre la fecha de incorporación o salida de los grupos y los roles en las actuaciones.

### Datos de Conciertos:

  - Almacenar detalles de conciertos, incluyendo la fecha, lugar, el conjunto de músicos y la lista de canciones interpretadas (setlist).

### Datos de Álbumes:

  - Información sobre álbumes, incluyendo el código GTIN, la lista de pistas y los detalles de producción.
  - Los álbumes pueden incluir grabaciones LIVE realizadas en conciertos.

## Tecnologías:
  
  - MongoDB: para almacenar la información de artistas, álbumes y conciertos. El proceso de migración se realizará utilizando pipelines en MongoDB.
  - Python: la librería NumPy para limpiar los datos proporcionados en formato CSV antes de la migración.

## Memoria:

  En la siguiente documento adjunto se encuentra la explicación del modelado UML y el perímetro de los agregados, la limpieza de datos que se ha seguido, la creación de los pipelines y las anomalías encontradas, y el planteamiento de un clúster.

[memoria.pdf](https://github.com/Ari-Potente/HMV-Music-Archiving-System/blob/main/Arquitectura%20de%20Datos_%20Memoria%20Final%20.pdf)
