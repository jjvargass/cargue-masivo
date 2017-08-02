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

#                     if not self.get_data_rubro(row['codigo']):
#                         self._logger.warning("*** Rubro: {0} no se encuentra en la bd ***".format(row['codigo']))
                    self.validate_format_rubro(row['codigo'])
#                     else:
#                         self._logger.warning("*** Existe Rubro: {0} en la bd ***".format(row['codigo']))

                except Exception as e:
                    self._logger.error('*******************')
                    self._logger.exception(e)

    def validate_format_rubro(self, rubro):
#         self._logger.debug(" {0} ".format(rubro))
        rubro_split = rubro.split("-")
        tamano = len(rubro_split)
#         self._logger.debug(" {0} ".format(tamano))
        if tamano >= 3:
            if len(rubro_split[2]) != 3 and len(rubro_split[2]) < 3:
                codigo = rubro_split[2];
                while len(codigo) < 3:
                    codigo = '0' + codigo
                rubro_split[2] = codigo
        #
        if tamano >= 4:
            if len(rubro_split[3]) != 2 and len(rubro_split[3]) < 2:
                codigo = rubro_split[3];
                while len(codigo) < 2:
                    codigo = '0' + codigo
                rubro_split[3] = codigo
        #
        if tamano >= 7:
            if len(rubro_split[6]) != 4 and len(rubro_split[6]) < 4:
                codigo = rubro_split[6];
                while len(codigo) < 4:
                    codigo = '0' + codigo
                rubro_split[6] = codigo
        new_rubro = '-'.join(rubro_split)
#         self._logger.debug(" {0} \n".format(new_rubro))
        return new_rubro

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

    def get_id_rubro(self, rubro):
        #self.get_naturaleza(cuenta_limpia)        
        try:
            self.cursor.execute("""
                select id
                from financiera.rubro
                where codigo = '{0}';""".format(rubro))
        except Exception as e:
            self._logger.error('********* get_id_rubro **********')
            self._logger.exception(e)
        rows = self.cursor.fetchone()
        if rows:
            return rows[0]
        else:
            return rows

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