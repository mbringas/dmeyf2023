## **📉 Predicción de bajas de paquete premium a partir de series temporales 💰**

En este trabajo hemos utilizado estrategias basadas en árboles 🌳 (DT, RF, XGBOOST) para predecir bajas de paquetes premium conociendo las caractersiticas
bancarias de clientes a lo largo del tiempo (saldos, créditos, consumos, mora en pago, etc.).

El dataset consta de una estructura vertical donde cada registro corresponde a un dado cliente en un cierto mes-año. Para estos, se presentan una variedad de 
caracteristicas respecto de su comportamiento bancario: saldos, consumos, créditos, mora en pago, cantidad de tarjetas, transferencias, cantidad de accesos al home banking,
recepción de sueldo, entre otras.

Una de las grandes dificultades de este problema radica en el desbalanceo del dataset ⚖️ : los casos positivos representan aproximadamente 5 de cada 100000.

A lo largo de las competencias, se fueron agregando meses disponibles para la predicción de la baja, y se fueron complejizando los modelos de ML utilizados.

Este trabajo fue llevado adelante durante la asignatura Data Mining aplicado a la Economia y Finanzas, en el segundo cuatrimestre del año 2023.
