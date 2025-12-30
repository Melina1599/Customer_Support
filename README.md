# Customer IT Support - Proyecto de Análisis de Datos

## Análisis y Descripción de la Base de Datos Transaccional

El origen de datos para este proyecto es el dataset "**Customer IT Support - Ticket Dataset**" de [Kaggle](www.kaggle.com). Este conjunto de datos simula un entorno real de gestión de incidencias de soporte de TI y está diseñado para tareas de **clasificación de texto** y **análisis de servicio al cliente multilingüe**.


### Estructura de Columnas Clave
La base de datos se caracteriza por columnas heterogéneas, enfocadas en capturar los detalles completos de un ticket de soporte.

- **Identificadores y Contexto:**
    - **Queue:** Departamento de destino (ej. Soporte Técnico, RRHH).
    - **Language:** Idioma del ticket (ej. es, en, de, fr, pt).
    - **Business type:** Contexto de la empresa cliente (ej. Tienda de tecnología).

- **Contenido del Ticket:**
    - **Subject (Asunto) y Body (Cuerpo):** Campos de texto libre que describen el problema.
    - **Answer:** La respuesta proporcionada por el agente, utilizada para el análisis de calidad.

- **Clasificación y Prioridad:**
    - **Priority:** Urgencia del ticket (Bajo, Medio, Crítico).
    - **Type:** Naturaleza de la solicitud (Incidente, Solicitud, Problema o Cambio).
    - **Tags:** Etiquetas específicas que categorizan la naturaleza técnica del problema.

- **Relaciones entre Datos:**
Los datos se presentan en un formato tabular plano (un solo archivo CSV), que actúa como una tabla de hechos desnormalizada. No existen relaciones explícitas de clave foránea en la fuente, pero las columnas Queue, Priority y Type actúan como atributos dimensionales que se utilizan para agrupar y filtrar los tickets.


En general las columnas son heterogeneas, pero para que sea mejor se eliminaron algunas, otras se renombraron y se agrego una columna en la unificacion origin para rastrear el dataset de origen


## Planificación del Análisis y Diseño del Esquema Estrella
**El objetivo principal del análisis se centra en el Análisis de Atención al Cliente.** Obtener insights sobre los problemas comunes, optimizar los procesos de soporte y mejorar la calidad general del servicio. Para lograr esto, se diseñó e implementó una base de datos analítica utilizando un esquema estrella en PostgreSQL.


### Diseño del Esquema Estrella:
El modelado desnormalizado facilita consultas analíticas rápidas al separar los datos en una tabla central de hechos y tablas de dimensiones que la rodean.

- **Tabla de Hechos:**
    - **fact_tickets:** Contiene una fila por cada ticket de soporte, almacenando las claves foráneas que enlazan con las dimensiones y los atributos de texto principales (subject, body, answer, business_type).

- **Tablas de Dimensiones:**
    - **dim_queue:** Almacena los departamentos únicos.
    - **dim_priority (priority, priority_level):** Almacena los niveles de urgencia.
    - **dim_category (category, subcategory, issue_detail):** Almacena la jerarquía de clasificación de los problemas.


## Preguntas Clave de Negocio (KPIs):
El dashboard se encuentra dividido en distintas pestañas para facilitar su comprensión, dependiendo del objetivo de las graficas.
- **General:** Proporciona una visión panorámica y de alto nivel sobre el estado actual del negocio Permiten entender el contexto global de los datos de un vistazo.

- **Rankings:** Enfocada en el análisis comparativo y de rendimiento. Permite identificar los elementos más y menos exitosos mediante listas de éxitos (Top N) y clasificaciones que ayudan a priorizar esfuerzos.

- **Segmentation:** Permite segmentar el volumen total de tickets por sector y por nivel de criticidad para identificar exactamente dónde residen los problemas más urgentes y qué áreas requieren atención inmediata. Esta vista es crucial para una gestión de incidentes y recursos eficiente.

- **Problems:** Enfocada en mostrar los problemas más habituales y, también, como se pierde información si el sistema no soporta ciertas caracteristicas.


## Estructura del proyecto
project/
├── data/
│   ├── raw/
│   └── processed_data/
├── docs/
│   └── scripts de creacion e insercion de datos en postgre
├── src/
│   ├── cleaning_scripts/
│   │   ├── __init__.py   
│   │   └── scripts de limpieza individuales
│   ├── data_cleaning_pipeine.py 
│   └── validate_master.py y load_to_postgres.py
├── README.md
├── archivos .ignore
└── archivos docker


## Enlaces a colab para EDA
* german
    * 5: https://colab.research.google.com/drive/1BW2bkIdiRn8jV9ROUzF5-wWUdKmdOKcf#scrollTo=xYLh0RtGTo3G
    * '0': https://colab.research.google.com/drive/14y_iLSFjMrDzH_oxh4kgOpQZn9bspTym#scrollTo=-cBvJy2b-rLS
* multi lang:
    * 3: https://colab.research.google.com/drive/1X19xsNd5w56P9wBA1d4L2DZ9AfbwsOVh#scrollTo=UM7kBZ9QGveh
    * 4: https://colab.research.google.com/drive/1SCWVtMKoc_IUISPXgLUThPH1d5sijgeh#scrollTo=Pe-WDL8rMTkF
    * 5: https://colab.research.google.com/drive/1lIlTul9gOMncnqcN8VK9fKOcRkLaNP0M#scrollTo=BUbnQNjCl_aO
* master: https://colab.research.google.com/drive/1X40E3OWrb_Iwmq0hFXZJpICchkWyUKGe?usp=sharing
    