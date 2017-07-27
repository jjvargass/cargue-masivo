#!/usr/bin/python
# -*- coding: utf-8 -*-

import psycopg2
import csv

class CuentaContable():
    def __init__(self, cursor, _logger, options):
        self.cursor = cursor
        self._logger = _logger
        self.options = options
    
    def check_existence_cuenta_contable(self):
        with open('csv/cuentas_contables.csv') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                try:
                    cuenta_limpia = self.clear_cuenta(row['codigo'])
                    ##self._logger.debug("*** Cuenta Contable: {0} ***".format(cuenta_limpia))
                    if not self.get_id_cuenta(cuenta_limpia):
                        self._logger.warning("*** La Cuenta Contable con código {0} no se encuentra en la bd ***".format(cuenta_limpia))
                        # crear
#                     else:
#                         #crear rubro
#                         self._logger.debug("*** Crear rubro ---- {0} ***".format('####'))                                                                     
                except Exception as e:
                    self._logger.error('*******************')
                    self._logger.exception(e)                    
                
    def get_id_cuenta(self, cuenta_limpia):
        #self.get_naturaleza(cuenta_limpia)        
        try:
            self.cursor.execute("""
                select id
                from financiera.cuenta_contable
                where codigo = '{0}';""".format(cuenta_limpia))
        except Exception as e:
            self._logger.error('********* get_id_cuenta **********')
            self._logger.exception(e)              
        rows = self.cursor.fetchone()
        if rows:
            return rows[0]
        else:
            return rows

    def get_data_cuenta(self, cuenta_limpia):
        try:
            self.cursor.execute("""
                select id, saldo, nombre, naturaleza, descripcion, codigo, nivel_clasificacion, cuenta_bancaria
                from financiera.cuenta_contable
                where codigo = '{0}';""".format(cuenta_limpia))
        except Exception as e:
            self._logger.error('********* get_data_cuenta **********')
            self._logger.exception(e)              
        all_rows = self.cursor.fetchall()
        if all_rows:
            #self._logger.debug("resultado: {0} ".format(all_rows[0]))
            # vericidad de cuenta
            self.check_validation(all_rows[0])
            return all_rows[0][0]
        else:
            #self._logger.debug("resultado: {0} ".format(all_rows))
            return None

    def check_validation(self, data_cuenta):
        cuenta = data_cuenta[5]
        naturaleza = self.get_naturaleza(cuenta)        
        nivel = self.get_nivel_cuenta(cuenta)
        self._logger.debug("resultado: {0} ".format(cuenta))
        if naturaleza != data_cuenta[3]:
            self._logger.warning("***Naturaleza de la cuenta {0} errada: Posee {1} y debe ser {2} ***".format(cuenta, data_cuenta[3], naturaleza))
        if nivel != data_cuenta[6]:
            self._logger.warning("***Nivel de clasificación de la cuenta {0} errada: Posee {1} y debe ser {2} ***".format(cuenta, data_cuenta[6], nivel))           
    
    def clear_cuenta(self, cuenta):
        #self._logger.debug("********************************* inicio clear_cuenta ***".format(cuenta))
        #self._logger.debug("*** {0} ***".format(cuenta))
        limpio = cuenta.split("-")
        #self._logger.debug("*** {0} ***".format(limpio))
        limpio2 = [x for x in limpio if x != '00']
        limpio_union = '-'.join(limpio2)
        #self._logger.debug("*** {0} ***".format(limpio_union))
        return limpio_union
        
    def get_nivel_cuenta(self, cuenta):
        cuenta_split = cuenta.split("-")
        nivel = len(cuenta_split)
        return nivel

    def get_naturaleza(self, cuenta):
        cuenta_split = cuenta.split("-") 
        naturaleza = cuenta_split[0]
        if naturaleza == '1' or naturaleza == '5' or naturaleza == '6' or naturaleza == '7' or naturaleza == '8':          
            #self._logger.debug("*** Numero: {0} ** Naturaleza: {1} ***".format(naturaleza, 'debito'))
            return 'debito'
        else:
            #self._logger.debug("*** Numero: {0} ** Naturaleza: {1} ***".format(naturaleza, 'credito'))
            return 'credito'
        
        