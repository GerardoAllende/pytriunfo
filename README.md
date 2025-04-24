Utilidad para procesar mails con pólizas de Triunfo Seguros
===========================================================

Triunfo seguros me envía pólizas por mail pero no las adjunta, en cambio envía un enlace que vence a los dos meses. Esto me causa problemas porque siempre olvido descargar los enlaces.

Esta utilidad se conecta por IMAP a una carpeta de mails, busca correos de Triunfo y descarga los archivos, los guarda en una base de datos de sqlite de forma eficiente porque hace diff binario contra el primer PDF así solamente guarda los cambios. Luego marca los mails como procesados para que la próxima vez no los tenga en cuenta. 

Se usa la librería pymupdf para leer la patente y fecha en el contenido del PDF y se usan estos datos para el nombre del PDF cuando se hace --extract. Además se usa esa librería para descomprimir los streams del PDF (salvo imágenes o fuentes) para que sea más eficiente el diff y los vuelve a comprimir cuando hace --extract.

La idea colocar este programa en un cron o tareas de windows para que se ejecute todos los días. En el momento de necesitar los PDF ejecutar con el argumento --extract. 

Requisitos:
-----------
* Python 3
* ```pip install -r requirements.txt```

Uso:
----
Primero editar pytriunfo.py y cambiar la configuración de servidor IMAP, usuario, contraseña y carpeta.

Procesar mails:
```
py pytruinfo.py
```

Extraer PDFs desde la base de datos al disco:
```
py pytriunfo.py --extract
```
