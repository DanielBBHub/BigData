from mrjob.job import MRJob
import re
from datetime import datetime

class MRIbex(MRJob):
    """ Mostrar las 5 acciones que mas han bajado en la  ́ultima semana y  ́ultimo mes. """


    def configure_args(self):
        super(MRIbex, self).configure_args()
    

    def mapper(self, key, line):
        data = line.split(",")
        accion = data[0]
        ultima_cotizacion = data[1]
        fecha = data[-1]



        yield("Cotizacion", (accion, ultima_cotizacion, fecha))
        

    def reducer(self, key, data):


        fecha_mes_dia_hora = datetime.now()

        mes_inicio = int(fecha_mes_dia_hora.month)- 1
        semana_inicio = int(fecha_mes_dia_hora.day)- 7
        
        
        mes_final = int(fecha_mes_dia_hora.month) 
        semana_final = int(fecha_mes_dia_hora.day) 
         
        valor_inicial_mes = {}
        valor_inicial_semana = {}
        maximo_incremento_mes = {}
        maximo_incremento_semana = {}
        for accion, ultima_cotizacion, fecha in data:

            mes_evaluado = int(fecha.split('/')[0])
            if mes_evaluado > mes_inicio and  mes_evaluado <= mes_final:

                ultimo_valor = round(float(ultima_cotizacion), 4)
                if accion not in valor_inicial_mes.keys():
                    valor_inicial_mes[accion] = ultimo_valor
                
                
                self.funcionMinMax(ultimo_valor, accion, maximo_incremento_mes)
                    
                dia_evaluado = int(fecha.split('/')[1])
                if dia_evaluado >= semana_inicio and  dia_evaluado <= semana_final:
                    if accion not in valor_inicial_semana.keys():
                        valor_inicial_semana[accion] = ultimo_valor

                    self.funcionMinMax(ultimo_valor, accion, maximo_incremento_semana)


        if len(maximo_incremento_mes) != 0 and len(maximo_incremento_semana) != 0:
            for (k,v), (k2,v2) in zip(maximo_incremento_mes.items(), maximo_incremento_semana.items()):
                maximo_incremento_mes[k] = round(v - valor_inicial_mes[k], 3)
                maximo_incremento_semana[k2] = round(v2 - valor_inicial_semana[k2], 3)
                
            ordenado_por_valor_mes = dict(sorted(maximo_incremento_mes.items(), key=lambda x:x[1]))
            ordenado_por_valor_semana = dict(sorted(maximo_incremento_semana.items(), key=lambda x:x[1]))
            
            i = 0
            for (k,v), (k2,v2) in zip(ordenado_por_valor_mes.items(), ordenado_por_valor_semana.items()):
                if i < 5:
                    yield("Cotizacion", (("ultimo mes:",k +" "+ str(v )),("ultima semana:", k2 +" "+ str(v2 ))))
                    i += 1

    def funcionMinMax(self, ultimo_valor, llave_accion, maximo_incremento):
        if len(maximo_incremento.items()) == 0:
            maximo_incremento[llave_accion] = ultimo_valor
        
        
        else:
            maximo_incremento[llave_accion] = ultimo_valor
            maximo_incremento = dict(sorted(maximo_incremento.items(), key=lambda x:x[1]))
            
                

if __name__=='__main__':
    MRIbex.run()

