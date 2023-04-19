from mrjob.job import MRJob
import re
from datetime import datetime

class MRIbex(MRJob):
    """ Dado el nombre de una accion, recuperar su valor mınimo y maximo de cotizacion de la
    ultima hora, semana y mes. """

    def configure_args(self):
        super(MRIbex, self).configure_args()
        self.add_passthru_arg('--nombre', default="FERROVIAL", help='Introduzca el nombre de la accion')
    

    def mapper(self, key, line):
        data = line.split(",")
        accion = data[0]
        ultima_cotizacion = data[1]
        hora_minuto = data[9]
        fecha = data[-1]



        yield(accion, (ultima_cotizacion, hora_minuto, fecha))
        

    def reducer(self, key, data):

        nombre_accion = self.options.nombre

        fecha_mes_dia_hora = datetime.now()
        dia_actual = int(fecha_mes_dia_hora.day)

        mes_inicio = int(fecha_mes_dia_hora.month)- 1
        semana_inicio = int(fecha_mes_dia_hora.day)- 7
        hora_inicio = int(fecha_mes_dia_hora.hour) - 1
        
        mes_final = int(fecha_mes_dia_hora.month) 
        semana_final = int(fecha_mes_dia_hora.day) 
        hora_final = int(fecha_mes_dia_hora.hour) 
        
        minimo = {"mes": 0, "semana": 0, "hora": 0}
        maximo = {"mes": 0, "semana": 0, "hora": 0}

        for ultima_cotizacion, hora_minuto, fecha in data:
            if key == nombre_accion:
                mes_evaluado = int(fecha.split('/')[0])
                if mes_evaluado > mes_inicio and  mes_evaluado <= mes_final:
                    ultimo_valor = round(float(ultima_cotizacion), 4)
                    self.funcionMinMax(ultimo_valor, minimo, maximo, "mes")
                    
                    dia_evaluado = int(fecha.split('/')[1])
                    if dia_evaluado >= semana_inicio and  dia_evaluado <= semana_final:
                        self.funcionMinMax(ultimo_valor, minimo, maximo, "semana")

                        hora_evaluada = int(hora_minuto.split(':')[0])
                        if hora_evaluada >= hora_inicio and  hora_evaluada <= hora_final and dia_evaluado == dia_actual:
                            self.funcionMinMax(ultimo_valor, minimo, maximo, "hora")

        if minimo["mes"] != 0:
            yield(key, (("ultimo mes:", minimo["mes"], maximo["mes"]),("ultima semana:", minimo["semana"], maximo["semana"]),("ultima hora:", minimo["hora"], maximo["hora"])))

    def funcionMinMax(self, ultimo_valor, minimo, maximo,  periodo):
        if minimo[periodo] == 0:
            minimo[periodo] = ultimo_valor
            maximo[periodo] = ultimo_valor
                    
        if ultimo_valor < minimo[periodo]:
            minimo[periodo] = ultimo_valor

        if ultimo_valor > maximo[periodo]:
            maximo[periodo] = ultimo_valor

if __name__=='__main__':
    MRIbex.run()

