#!/usr/bin/python
# -*- coding: utf-8 -*-

import csv
import logging
import erppeek
from optparse import OptionParser
from connection import Connection
from cuenta_contable import CuentaContable
from rubro import Rubro
from concepto import Concepto

logging.basicConfig()
_logger = logging.getLogger('IMPORT')

def main():

    usage = "load conceptos file csv to kronos: %prog [options]"
    parser = OptionParser(usage)

    parser.add_option("-N", "--db_name", dest="db_name", help="database name", default="financiera")
    parser.add_option("-U", "--db_user",dest="db_user",help="database user", default="postgres")
    parser.add_option("-P", "--db_password", dest="db_password", help="database password", default="postgres")
    parser.add_option("-H", "--host_serverBD", dest="host_serverBD", help="server host", default="localhost")
    parser.add_option("-K", "--port_serverBD", dest="port_serverBD", help="server port", default="5432")
    parser.add_option("-p", "--path_csv", dest="path_csv", help="path of file for uploading", default="csv/concepto.csv")
    parser.add_option("-d", "--debug", dest="debug", help="Mostrar mensajes de debug utilize 10", default=10)

    (options, args) = parser.parse_args()
    _logger.setLevel(int(options.debug))

    if not options.db_name:
        parser.error('Parametro db_name no especificado')
    if not options.db_user:
        parser.error('Parametro db_user no especificado') 
    if not options.db_password:
        parser.error('Parametro db_password no especificado')
    if not options.host_serverBD:
        parser.error('Parametro host_serverBD no especificadon')
    if not options.port_serverBD:
        parser.error('Parametro host_serverBD no especificadon')
    if not options.path_csv:
        parser.error('Parametro path_csv no especificadon')

    connect = Connection(options)
    postgres_connect = connect.get_connection()
    cursor = connect.get_cursor()

    # Cuentas Contables
#     cuentas = CuentaContable(cursor, _logger, options)
#     cuentas.check_existence_cuenta_contable()

    # Rubro
#     rubro = Rubro(cursor, _logger, options)
#     rubro.check_existence_rubro()

    # Conceptos
    concepto = Concepto(cursor, _logger, options, postgres_connect)
    operara = concepto.check_existence_rubro_and_cuentas()
    if operara:
        _logger.warning("********* Verificación Exitosa: {0} *********\n".format(operara))
        #concepto.register_concepto()
    else:
        _logger.warning("********* Verificación Fallida : {0} *********".format(operara))

if __name__ == '__main__':
    main()