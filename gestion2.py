from mrjob.job import MRJob
import re
import datetime

class MRIbex(MRJob):
    """ Generar un listado mensual (del mes actual) donde se indique, para cada accion, su valor
    inicial, final, mınimo y maximo. """

    def mapper(self, key, line):
        data = line.split(",")
        accion = data[0]
        ultima_cotizacion = data[1]
        fecha = data[-1]



        yield(accion, (ultima_cotizacion, fecha))
        

    def reducer(self, key, data):
        mes_evaluado = 0
        valor_inicial = 0
        valor_final = 0
        minimo = 0
        maximo = 0
        for ultima_cotizacion, fecha in data:
            mes = fecha.split('/')[0]
            if mes_evaluado == 0:
                mes_evaluado = int(mes)
                valor_inicial = ultima_cotizacion
                minimo = ultima_cotizacion
                maximo = ultima_cotizacion

            if int(mes) == mes_evaluado:
                if ultima_cotizacion < minimo:
                    minimo = ultima_cotizacion

                if ultima_cotizacion > maximo:
                    maximo = ultima_cotizacion 

            valor_final = ultima_cotizacion 

        yield(key, (mes_evaluado,valor_inicial, valor_final, minimo, maximo))

        
if __name__=='__main__':
    MRIbex.run()

