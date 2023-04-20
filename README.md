# BigData  
Los archivos desarrollados cumplen los siguientes propositos:  
  
  -gestion1.py: "Generar un listado semanal (de la semana actual) donde se indique, para cada accion, su valor inicial, final, mınimo y maximo."  
  -gestion2.py: "Generar un listado mensual (del mes actual) donde se indique, para cada accion, su valor inicial, final, mınimo y maximo."  
  -gestion3.py: "Dado el nombre de una accion y un rango de fechas, obtener su valor mınimo y maximo de cotizacion, ası como el el porcentaje de decremento y de incremento desde el valor inicial de cotizacion hasta el mınimo y maximo, respectivamente"  
  -gestion4.py: "Dado el nombre de una acci ́on, recuperar su valor mınimo y maximo de cotizacion de la ́ultima hora, semana y mes."  
  -gestion5.py: "Mostrar las 5 acciones que m ́as han subido en la ́ultima semana y ́ultimo mes"  
  -gestion6.py: "Mostrar las 5 acciones que m ́as han bajado en la ultima semana y ́ultimo mes."  
  
Para ejecutar los archivos en local se ha de copiar y pegar en la consola, en el directorio en los que se encuentren:  
<pre><code>python gestionX.py dia1_fecha.csv dia2_fecha.csv dia3_fecha.csv dia4_fecha.csv</code></pre>  
  
Para ejecutar los archivos dentro de HDFS, primero tendremos que iniciar los servicios relacionados:  
<pre><code>start-dfs.sh</code></pre>  
<pre><code>start-yarn.sh</code></pre>   

Los archivos generados se han guardado dentro de un directorio "Ibex" con: 
<pre><code>"Para crear el directorio Ibex"</code>
<code>hdfs dfs -mkdir Ibex</code></pre>
<pre><code>"Para copiar en el directorio Ibex"</code>
<code>hdfs dfs -put diaY_fecha.csv Ibex</code></pre>   
  
Una vez guardados, los programas se ejecutan dentro de hadoop con los siguientes comandos:  
<pre><code>"Sustituir la "X" (de 1-6) en gestionX.py y las Y (de 1-4) de todos los archivos de informacion"</code>  
<code>python gestionX.py -r hadoop hdfs:///user/alumno/Ibex/diaY_fecha.csv --output-dir Ibex/info_ibex</code></pre>
