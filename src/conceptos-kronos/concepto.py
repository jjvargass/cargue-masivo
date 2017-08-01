#!/usr/bin/python
# -*- coding: utf-8 -*-

from psycopg2 import *
import csv
from cuenta_contable import CuentaContable
from rubro import Rubro

class Concepto():
    def __init__(self, cursor, _logger, options, connect):
        self.cursor = cursor
        self._logger = _logger
        self.options = options
        self.connect = connect

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
                    # Rubro
                    if row['codigo_rubro']:
                        if not rubro.get_data_rubro(row['codigo_rubro']):
                            self._logger.warning("*** Concepto: {0} {1} ***".format(row['codigo'], row['nombre']))
                            self._logger.warning("****** Rubro: {0} no se encuentra en la bd ******".format(row['codigo_rubro']))
                            validacion_exitosa = False
                    #Cuentas Contables
                    # debito
                    if row['cuenta_contable_debito']:
                        self._logger.debug("*** Concepto: {0} {1} ***".format(row['codigo'], row['nombre']))
                        if not cuentas.get_id_cuenta(cuentas.clear_cuenta(row['cuenta_contable_debito'])):
                            self._logger.warning("********* Cuenta debito: {0} no se encuentra en la bd *********".format(row['cuenta_contable_debito']))
                            validacion_exitosa = False
#                         elif 'debito' != cuentas.get_naturaleza(row['cuenta_contable_debito']):
#                             self._logger.warning("********* Cuenta : {0} no Corresponce a la naturaleza debito *********".format(row['cuenta_contable_debito']))
#                             validacion_exitosa = False
                    # credito
                    if row['cuenta_contable_credito']:
                        if not cuentas.get_id_cuenta(cuentas.clear_cuenta(row['cuenta_contable_credito'])):
                            self._logger.warning("********* Cuenta credito': {0} no se encuentra en la bd *********".format(row['cuenta_contable_credito']))
                            validacion_exitosa = False
#                         elif 'credito' != cuentas.get_naturaleza(row['cuenta_contable_credito']):
#                             self._logger.warning("********* Cuenta : {0} no Corresponce a la naturaleza credito *********".format(row['cuenta_contable_credito']))
#                             validacion_exitosa = False
                except Exception as e:
                    self._logger.error('************* check_existence_rubro_and_cuentas *************')
                    self._logger.exception(e)
            self._logger.debug("+++ Fin Verifica existencia de rubros y cuentas contables que se asociaran al concepto +++")
            return validacion_exitosa
 
    def register_concepto(self):
        self._logger.debug("+++ Registra Conceptos +++")
        with open('csv/concepto.csv') as csvfile:
            reader = csv.DictReader(csvfile)
            padre_id = None
            for row in reader:
                try:
                    hijo_id = None
                    self._logger.debug("*** Concepto: {0} {1} ***".format(row['codigo'], row['nombre']))
                    if row['padre']:
                        padre_id = self.add_concepto('', row['padre'], row['codigo'], row['nombre'], row['fecha_creacion'], row['cabeza'], row['fecha_expiracion'], row['descripcion'], row['tipo_concepto'], row['codigo_rubro'], row['cuenta_contable_debito'], row['cuenta_contable_credito'])
                        #self._logger.debug("*** Padre: {0} ***".format(padre_id))
                    else:
                        # crear concepto
                        hijo_id = self.add_concepto(padre_id, row['padre'], row['codigo'], row['nombre'], row['fecha_creacion'], row['cabeza'], row['fecha_expiracion'], row['descripcion'], row['tipo_concepto'], row['codigo_rubro'], row['cuenta_contable_debito'], row['cuenta_contable_credito'])
                        # registrar_agectacion
                        self.register_afectacion(row['afectacion_presupuesto_ingreso'], row['afectacion_presupuesto_egreso'],'1',hijo_id) #1 presupuesto / 2 contabilidad
                        self.register_afectacion(row['afectacion_contabilidad_ingreso'], row['afectacion_contabilidad_egreso'],'2',hijo_id) #1 presupuesto / 2 contabilidad
                        # registrar cuentas contables
                        self.register_concepto_cuenta_contable(row['cuenta_contable_debito'], hijo_id)
                        self.register_concepto_cuenta_contable(row['cuenta_contable_credito'], hijo_id)

                    # registratr gerarquia
                    if padre_id and hijo_id:
                        self.register_geraquia(padre_id, hijo_id)

                except Exception as e:
                    self._logger.error('************* register_concepto *************')
                    self._logger.exception(e)
            self._logger.debug("+++ Fin Registra Conceptos +++")

    def add_concepto(self, padre_id, padre, codigo, nombre, fecha_creacion, cabeza, fecha_expiracion, descripcion, tipo_concepto, codigo_rubro, cuenta_contable_debito, cuenta_contable_credito):
        rubro = Rubro(self.cursor, self._logger, self.options)
        rubro_id = rubro.get_id_rubro(codigo_rubro)
        if padre:
            if len(descripcion) == 0:
                descripcion = nombre
            if len(fecha_expiracion) == 0:
                sql = """
                    insert into financiera.concepto(codigo, nombre, fecha_creacion, cabeza, descripcion, tipo_concepto)
                    values
                    ('{0}','{1}','{2}',{3},'{4}',{5}) RETURNING id;""".format(codigo, nombre, fecha_creacion, cabeza, descripcion, tipo_concepto)
            else:
                sql = """
                    insert into financiera.concepto(codigo, nombre, fecha_creacion, cabeza, fecha_expiracion, descripcion, tipo_concepto)
                    values
                    ('{0}', '{1}','{2}',{3},'{4}',{5},{6}) RETURNING id;""".format(codigo, nombre, fecha_creacion, cabeza, fecha_expiracion, descripcion, tipo_concepto)
            try:
                self.cursor.execute(sql)
                self.connect.commit()
            except Exception as e:
                self._logger.error('********* add_concepto padre **********')
                self._logger.exception(e)
                self.connect.rollback()()
                return None
            else:
                rows = self.cursor.fetchone()
                if rows:
                    return rows[0]
                else:
                    return rows
        else:
            if len(descripcion) == 0:
                descripcion = nombre
            if len(fecha_expiracion) == 0:
                sql = """
                    insert into financiera.concepto(codigo, nombre, fecha_creacion, cabeza, descripcion, tipo_concepto, rubro)
                    values
                    ('{0}','{1}','{2}',{3},'{4}',{5}, {6}) RETURNING id;""".format(codigo, nombre, fecha_creacion, cabeza, descripcion, tipo_concepto, rubro_id)
            else:
                sql = """
                    insert into financiera.concepto(codigo, nombre, fecha_creacion, cabeza, fecha_expiracion, descripcion, tipo_concepto, rubro)
                    values
                    ('{0}', '{1}','{2}',{3},'{4}',{5},{6}, {7}) RETURNING id;""".format(codigo, nombre, fecha_creacion, cabeza, fecha_expiracion, descripcion, tipo_concepto, rubro_id)
            try:
                self.cursor.execute(sql)
                self.connect.commit()
            except Exception as e:
                self._logger.error('********* add_concepto padre **********')
                self._logger.exception(e)
                self.connect.rollback()()
                return None
            else:
                rows = self.cursor.fetchone()
                if rows:
                    return rows[0]
                else:
                    return rows

    def register_afectacion(self, ingreso, egreso, tipo_afectacion, id_concepto):
        data_insert = ()
        if tipo_afectacion == '1':
            if len(ingreso) == 0 and len(egreso) == 0:
                data_insert = ('FALSE', 'FALSE', id_concepto, 1)
            elif len(ingreso) != 0 and len(egreso) == 0:
                data_insert = ('TRUE', 'FALSE', id_concepto, 1)
            elif len(ingreso) == 0 and len(egreso) != 0:
                data_insert = ('FALSE', 'TRUE', id_concepto, 1)
            else:
                data_insert = ('TRUE', 'TRUE', id_concepto, 1)
        else:
            if len(ingreso) == 0 and len(egreso) == 0:
                data_insert = ('FALSE', 'FALSE', id_concepto, 2)
            elif len(ingreso) != 0 and len(egreso) == 0:
                data_insert = ('TRUE', 'FALSE', id_concepto, 2)
            elif len(ingreso) == 0 and len(egreso) != 0:
                data_insert = ('FALSE', 'TRUE', id_concepto, 2)
            else:
                data_insert = ('TRUE', 'TRUE', id_concepto, 2)
        sql = """
        insert into financiera.afectacion_concepto(afectacion_ingreso, afectacion_egreso, concepto, tipo_afectacion)
        values
        ({0[0]}, {0[1]}, {0[2]}, {0[3]}) RETURNING id;""".format(data_insert)
        try:
            self.cursor.execute(sql)
            self.connect.commit()
        except Exception as e:
            self._logger.error('********* register_afectacion **********')
            self._logger.exception(e)
            self.connect.rollback()()

    def register_geraquia(self, padre, hijo):
        sql = """
        insert into financiera.concepto_concepto(concepto_padre, concepto_hijo)
        values
        ({0}, {1}) RETURNING id;""".format(padre, hijo)
        try:
            self.cursor.execute(sql)
            self.connect.commit()
        except Exception as e:
            self._logger.error('********* register_geraquia **********')
            self._logger.exception(e)
            self.connect.rollback()()

    def register_concepto_cuenta_contable(self, cuenta_contable, concepto_id):
        #Cuentas Contables
        cuentas = CuentaContable(self.cursor, self._logger, self.options)
        cuenta_id = cuentas.get_id_cuenta(cuentas.clear_cuenta(cuenta_contable))
        sql = """
        insert into financiera.concepto_cuenta_contable(cuenta_contable, concepto, cuenta_acreedora)
        values
        ({0}, {1}, 'FALSE');""".format(cuenta_id, concepto_id)
        try:
            self.cursor.execute(sql)
            self.connect.commit()
        except Exception as e:
            self._logger.error('********* register_concepto_cuenta_contable **********')
            self._logger.exception(e)
            self.connect.rollback()()
