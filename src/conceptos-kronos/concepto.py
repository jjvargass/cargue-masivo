#!/usr/bin/python
# -*- coding: utf-8 -*-

import psycopg2
import csv
from cuenta_contable import CuentaContable
from rubro import Rubro

class Concepto():
    def __init__(self, cursor, _logger, options):
        self.cursor = cursor
        self._logger = _logger
        self.options = options

    def check_existence_rubro_and_cuentas(self):
        self._logger.debug("+++ Verifica existencia de rubros y cuentas contables que se asociaran al concepto +++")
        with open('csv/concepto.csv') as csvfile:
            reader = csv.DictReader(csvfile)
            # Rubro
            rubro = Rubro(self.cursor, self._logger, self.options)
            #Cuentas Contables
            cuentas = CuentaContable(self.cursor, self._logger, self.options)
            validacion_exitosa = True
            for row in reader:
                try:
                    #self._logger.debug("*** Concepto: {0} {1} ***".format(row['codigo'], row['nombre']))
                    # Rubro
                    if row['codigo_rubro']:
                        if not rubro.get_data_rubro(row['codigo_rubro']):
                            self._logger.warning("*** Concepto: {0} {1} ***".format(row['codigo'], row['nombre']))
                            self._logger.warning("****** Rubro: {0} no se encuentra en la bd ******".format(row['codigo_rubro']))
                            validacion_exitosa = False
                    #Cuentas Contables
                    if row['cuenta_contable_debito']:
                        if not cuentas.get_id_cuenta(cuentas.clear_cuenta(row['cuenta_contable_debito'])):
                            self._logger.warning("*** Concepto: {0} {1} ***".format(row['codigo'], row['nombre']))
                            self._logger.warning("********* Cuenta debito: {0} no se encuentra en la bd *********".format(row['cuenta_contable_debito']))
                            validacion_exitosa = False
                    if row['cuenta_contable_credito']:
                        if not cuentas.get_id_cuenta(cuentas.clear_cuenta(row['cuenta_contable_credito'])):
                            self._logger.warning("*** Concepto: {0} {1} ***".format(row['codigo'], row['nombre']))
                            self._logger.warning("********* Cuenta credito': {0} no se encuentra en la bd *********".format(row['cuenta_contable_credito']))
                            validacion_exitosa = False
                except Exception as e:
                    self._logger.error('************* check_existence_rubro_and_cuentas *************')
                    self._logger.exception(e)
            self._logger.debug("+++ Fin Verifica existencia de rubros y cuentas contables que se asociaran al concepto +++")
            return validacion_exitosa
 
        def saludo(self):
            self._logger.debug("+++ Verifica existencia de rubros y cuentas contables que se asociaran al concepto +++")
            print "hola"