from mrjob.job import MRJob
import re
import datetime

class MRIbex(MRJob):
    """ Dado el nombre de una accion y un rango de fechas, obtener su valor mınimo y maximo de
    cotizacion, ası como el el porcentaje de decremento y de incremento desde el valor inicial de
    cotizacion hasta el mınimo y maximo, respectivamente. """
    def configure_args(self):
        super(MRIbex, self).configure_args()
        self.add_passthru_arg('--nombre', default="ENAGAS", help='Introduzca el nombre de la accion:')
        self.add_passthru_arg('--fechainicio', default="04/11", help='Introduzca la fecha de inicio [mm/dd]:')
        self.add_passthru_arg('--fechafin', default="04/18", help='Introduzca la fecha de fin [mm/dd]:')


    def mapper(self, key, line):
        data = line.split(",")
        accion = data[0]
        ultima_cotizacion = data[1]
        fecha = data[-1]



        yield(accion, (ultima_cotizacion, fecha))
        

    def reducer(self, key, data):

        nombre_accion = self.options.nombre

        inicio_fecha = self.options.fechainicio
        mes_inicio = inicio_fecha.split('/')[0]
        dia_inicio = inicio_fecha.split('/')[1]

        fin_fecha = self.options.fechafin
        mes_final = fin_fecha.split('/')[0]
        dia_final = fin_fecha.split('/')[1]

        valor_inicial = 0
        valor_final = 0
        minimo = 0
        maximo = 0

        for ultima_cotizacion, fecha in data:

            mes_evaluado = fecha.split('/')[0]
            if mes_evaluado >= mes_inicio and  mes_evaluado <= mes_final:

                dia_evaluado = fecha.split('/')[1]
                if dia_evaluado >= dia_inicio and  dia_evaluado <= dia_final:

                    if key == nombre_accion:
                        ultimo_valor = round(float(ultima_cotizacion), 4)

                        if valor_inicial == 0:
                            valor_inicial = ultimo_valor
                            minimo = valor_inicial
                            maximo = valor_inicial

                        if ultimo_valor < minimo:
                            minimo = ultimo_valor

                        if ultimo_valor > maximo:
                            maximo = ultimo_valor 

        if minimo != 0:
            yield(key, (minimo, maximo, (valor_inicial / minimo)*100 - 100,100 - (valor_inicial / maximo)*100 ))
        

        
if __name__=='__main__':
    MRIbex.run()

