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
        with open('csv/cuentas_contables_test.csv') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                try:
                    cuenta_limpia = self.clear_cuenta(row['codigo'])  
                    self._logger.debug("*** Cuenta Contable: {0} ***".format(cuenta_limpia))
                    if not self.get_id_cuenta(row, cuenta_limpia):
                        self._logger.debug("*** El codigo {0} de tipo {1} no se encuentra en la bd ***".format(cuenta_limpia, row['tipo']))
                        # crear
                    else:
                        #crear rubro
                        self._logger.debug("*** Crear rubro ---- {0} ***".format('####'))
                                                                     
                except Exception as e:
                    self._logger.error('*******************')
                    self._logger.exception(e)                    
                
    def get_id_cuenta(self, row, cuenta_limpia):
        try:
            self.cursor.execute("""
                select id
                from financiera.cuenta_contable
                where naturaleza = '{0}'
                and  codigo = '{1}';""".format(row['tipo'], cuenta_limpia))
        except Exception as e:
            self._logger.error('********* get_id_cuenta **********')
            self._logger.exception(e)              
        rows = self.cursor.fetchone()
        if rows:
            return rows[0]
        else:
            return rows        

    
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
        
        