#!/usr/bin/python
# -*- coding: utf-8 -*-

import psycopg2
import csv

class Rubro():
    def __init__(self, cursor, _logger, options):
        self.cursor = cursor
        self._logger = _logger
        self.options = options

    def check_existence_rubro(self):
        with open('csv/rubro.csv') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                try:
#                     self._logger.debug("*************************** Rubros: {0} *************************** ".format(row['codigo']))
#                     if not self.check_phater_rubro(row['codigo']):
#                         self._logger.warning("*** Rubro: {0} no cumple garantia padre hijo ***".format(row['codigo']))
#                     else:
#                         self._logger.warning("*** Existe Rubro: {0} en la bd ***".format(row['codigo']))

                    if not self.get_data_rubro(row['codigo']):
                        self._logger.warning("*** Rubro: {0} no se encuentra en la bd ***".format(row['codigo']))
#                     else:
#                         self._logger.warning("*** Existe Rubro: {0} en la bd ***".format(row['codigo']))

                except Exception as e:
                    self._logger.error('*******************')
                    self._logger.exception(e)

    def get_data_rubro(self, rubro):
        try:
            self.cursor.execute("""
                select *
                from financiera.rubro
                where codigo = '{0}';""".format(rubro))
        except Exception as e:
            self._logger.error('********* get_data_rubro **********')
            self._logger.exception(e)
        all_rows = self.cursor.fetchall()
        if all_rows:
            #self._logger.debug("resultado: {0} ".format(all_rows[0]))
            return all_rows[0][0]
        else:
            #self._logger.debug("resultado: {0} ".format(all_rows))
            return None

    def check_phater_rubro(self, rubro):
        #self._logger.debug(" {0} ".format(rubro))
        rubro_split = rubro.split("-")
        tamano = len(rubro_split)
        if tamano == 1:
            #self._logger.debug("llego a la raiz  {0} ".format(rubro))
            return 'OK'
        else:
            #verifico que esista
            if self.get_data_rubro(rubro):
                #self._logger.debug(" {0} ".format('recursivo'))
                #buscar padre
                del rubro_split[-1]
                rubro1 = '-'.join(rubro_split)
                return self.check_phater_rubro(rubro1)
            else:
                #self._logger.warning("Garantia de padre hijo. El rubro {0} no existe en bd".format(rubro))
                return None