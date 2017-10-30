
from psycopg2 import *
import csv
from cuenta_contable import CuentaContable
from rubro import Rubro

class Homologacion():
    def __init__(self, cursor, _logger, options, connect):
        self.cursor = cursor
        self._logger = _logger
        self.options = options
        self.connect = connect

    def register_homologacion(self):
        self._logger.debug("+++ Registra homologacion +++")
        with open(self.options.path_csv) as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                try:
                    self._logger.debug("*** Concepto Kronos: {0}  -  Concepto Titan: {1} ***".format(row['concepto_kronos'], row['concepto_titan']))
                    self.add_homologaicon(row['vigencia'], row['fecha_creacion'], row['concepto_kronos'], row['concepto_titan'])
                except Exception as e:
                    self._logger.error('************* register_homologacion *************')
                    self._logger.exception(e)
            self._logger.debug("+++ Fin Registra homologacion +++")

    def add_homologaicon(self, vigencia, fecha_creacion, concepto_kronos, concepto_titan):
        sql = """
        insert into financiera.homologacion_concepto(vigencia, fecha_creacion, concepto_kronos, concepto_titan)
        values
        ({0}, '{1}', {2}, {3}) RETURNING id;""".format(vigencia, fecha_creacion, concepto_kronos, concepto_titan)
        try:
            self.cursor.execute(sql)
            self.connect.commit()
        except Exception as e:
            self._logger.error('********* register_facultad_proyecto **********')
            self._logger.exception(e)
            self.connect.rollback()()