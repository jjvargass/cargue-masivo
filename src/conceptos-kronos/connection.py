#!/usr/bin/python
# -*- coding: utf-8 -*-

import psycopg2

class Connection():
    def __init__(self, options):
        self.connection = None
        self.cursor = None
        self.options = options

    def get_connection(self):
        dbname = self.options.db_name
        user = self.options.db_user
        password = self.options.db_password
        host = self.options.host_serverBD
        if self.connection == None:
            cadena_conect = "dbname={} user={} password={} host={} ".format(dbname, user, password, host)
            try:
                self.connection = psycopg2.connect(cadena_conect)
            except Exception as e:
                print "I am unable to connect to the database"
                print e       
        return self.connection
    
    def get_cursor(self):
        if self.cursor == None and self.connection != None:
            self.cursor = self.connection.cursor()
        return self.cursor
 