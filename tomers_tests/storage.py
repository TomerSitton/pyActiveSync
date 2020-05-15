########################################################################
#  Copyright (C) 2013 Sol Birnbaum
# 
#  This program is free software; you can redistribute it and/or
#  modify it under the terms of the GNU General Public License
#  as published by the Free Software Foundation; either version 2
#  of the License, or (at your option) any later version.
# 
#  This program is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
# 
#  You should have received a copy of the GNU General Public License
#  along with this program; if not, write to the Free Software
#  Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
#  MA  02110-1301, USA.
########################################################################

import sqlite3

class storage:
    @staticmethod
    def set_keyvalue(key, value, path="pyas.asdb"):
        conn = sqlite3.connect(path)
        curs = conn.cursor()
        curs.execute("INSERT INTO KeyValue VALUES ('%s', '%s')" % (key, value))
        conn.commit()
        conn.close()
    
    @staticmethod
    def update_keyvalue(key, value, path="pyas.asdb"):
        conn = sqlite3.connect(path)
        curs = conn.cursor()
        sql = "UPDATE KeyValue SET Value='%s' WHERE Key='%s'" % (value.replace("'","''"), key)
        curs.execute(sql)
        conn.commit()
        conn.close()

    @staticmethod
    def get_keyvalue(key, path="pyas.asdb"):
        conn = sqlite3.connect(path)
        curs = conn.cursor()
        curs.execute("SELECT Value FROM KeyValue WHERE Key='%s'" % key)
        try:
            value = curs.fetchone()[0]
            conn.close()
            return value
        except:
            conn.close()
            return None

    @staticmethod
    def create_db(path=None):
        """
        Create a DB with the following tables:

        1. FolderHierarchy - saves data about the folder hierarchy:
            a. ServerID - The server-unique identifier for a folder on the server (as specified in AS-ASCMD)
            b. ParentID - The server ID of a parent folder of the folder specified by ServerID
            c. DisplayName - The str name of the folder
            d. Type - the type of the folder (as specified in AS-ASCMD). list of folder types:
                '1' - User-created folder (generic)
                '2' - Default Inbox folder
                '3' - Default Drafts folder
                '4' - Default Deleted Items folder
                '5' - Default Sent Items folder
                '6' - Default Outbox folder
                '7' - Default Tasks folder
                '8' - Default Calendar folder
                '9' - Default Contacts folder
                '10' - Default Notes folder
                '11' - Default Journal folder
                '12' - User-created Mail folder
                '13' - User-created Calendar folder
                '14' - User-created Contacts folder
                '15' - User-created Tasks folder
                '16' - User-created journal folder
                '17' - User-created Notes folder
                '18' - Unknown folder type
                '19' - Recipient information cache

        2. SyncKeys - saves the Synchronization Keys for synced collections:
            a. SyncKey - the Synchronization Key of a collection
            b. CollectionID - the ID of a collection

        3. KeyValue - key-value data: #TODO - check what is this table and is it necessary
            a. Key - The key
            b. Value - The value

        4. MSASEMAIL - saves mails data. A lot of rows. some of them:
            a. ServerId - the id of the folder containing that mail #TODO - check if true
            b. email_To - for whom it was sent
            c. email_Cc - cc of the mail
            d. email_From - who sent it
            e. etc...

        This function also index the tables
        :param path: The path of the DB
        :return: None
        """
        if path:
            if path != "pyas.asdb":
                if not path[-1] == "\\":
                    path = path + "\\pyas.asdb"
        else:
            path="pyas.asdb"
        conn = sqlite3.connect(path)
        curs = conn.cursor()
        curs.execute("""CREATE TABLE FolderHierarchy (ServerId text, ParentId text, DisplayName text, Type text)""")
        curs.execute("""CREATE TABLE SyncKeys (SyncKey text, CollectionId text)""")
        curs.execute("""CREATE TABLE KeyValue (Key text, Value blob)""")

        curs.execute("""CREATE TABLE MSASEMAIL (ServerId text, 
                                                email_To text, 
                                                email_Cc text,
                                                email_From text,
                                                email_Subject text,
                                                email_ReplyTo text,
                                                email_DateReceived text,
                                                email_DisplayTo text,
                                                email_ThreadTopic text,
                                                email_Importance text,
                                                email_Read text,
                                                airsyncbase_Attachments text,
                                                airsyncbase_Body text,
                                                email_MessageClass text,
                                                email_InternetCPID text,
                                                email_Flag text,
                                                airsyncbase_NativeBodyType text,
                                                email_ContentClass text,
                                                email2_UmCallerId text,
                                                email2_UmUserNotes text,
                                                email2_ConversationId text,
                                                email2_ConversationIndex text,
                                                email2_LastVerbExecuted text,
                                                email2_LastVerbExecutedTime text,
                                                email2_ReceivedAsBcc text,
                                                email2_Sender text,
                                                email_Categories text,
                                                airsyncbase_BodyPart text,
                                                email2_AccountId text,
                                                rm_RightsManagementLicense text)""")
        
        conn.commit()

        indicies = ['CREATE UNIQUE INDEX "main"."MSASEMAIL_ServerId_Idx" ON "MSASEMAIL" ("ServerId" ASC)', 
                    'CREATE UNIQUE INDEX "main"."SyncKey_CollectionId_Idx" ON "SyncKeys" ("CollectionId" ASC)',
                    'CREATE UNIQUE INDEX "main"."KeyValue_Key_Idx" ON "KeyValue" ("Key" ASC)',
                    'CREATE UNIQUE INDEX "main"."FolderHierarchy_ServerId_Idx" ON "FolderHierarchy" ("ServerId" ASC)',
                    'CREATE  INDEX "main"."FolderHierarchy_ParentType_Idx" ON "FolderHierarchy" ("ParentId" ASC, "Type" ASC)',
                    ]
        for index in indicies:
            curs.execute(index)
        #storage.set_keyvalue("X-MS-PolicyKey", "0")   --- this is used for policy. part of the Provision command. i think it is not necessary
        #storage.set_keyvalue("EASPolicies", "")        --- this is used for policy. part of the Provision command. i think it is not necessary
        #storage.set_keyvalue("MID", "0")               --- mail id...? i think its used just for sending mails
        conn.commit()

        conn.close()
    
    @staticmethod
    def get_conn_curs(path="pyas.asdb"):
        conn = sqlite3.connect(path)
        curs = conn.cursor()
        return conn, curs

    @staticmethod
    def close_conn_curs(conn):
        try:
            conn.commit()
            conn.close()
        except:
            return False
        return True


    @staticmethod
    def insert_folderhierarchy_change(folder, curs):
        sql = """INSERT INTO FolderHierarchy VALUES ("%s", "%s", "%s", "%s")""" % (folder.ServerId, folder.ParentId, folder.DisplayName, folder.Type)
        curs.execute(sql)

    @staticmethod
    def update_folderhierarchy_change(folder, curs):
        sql = "UPDATE FolderHierarchy SET ParentId='%s', DisplayName='%s', Type='%s' WHERE ServerId == '%s'""" % (folder.ParentId, folder.DisplayName, folder.Type, folder.ServerId)
        curs.execute(sql)

    @staticmethod
    def delete_folderhierarchy_change(folder, curs):
        #Command only sent we permement delete is requested. Otherwise it would be 'Update' to ParentId='3' (Deleted Items).
        #sql = "UPDATE FolderHierarchy SET ParentId='4' WHERE ServerId == '%s'""" % (folder.ServerId)
        sql = "DELETE FROM MSASEMAIL WHERE ServerId like '%s:%%'" % (folder.ServerId)
        curs.execute(sql)
        sql = "DELETE FROM FolderHierarchy WHERE ServerId == '%s'" % (folder.ServerId)
        curs.execute(sql)

    @staticmethod
    def update_folderhierarchy(changes, path="pyas.asdb"):
        """
        :param changes: list of tuples of command and objects.
                        for example:
                        [('ADD', <objects.MSASCMD.FolderHierarchy.Folder object>),
                        ('ADD', <objects.MSASCMD.FolderHierarchy.Folder object>)]
        :param path:
        :return:
        """
        conn = sqlite3.connect(path)
        curs = conn.cursor()
        for change in changes:
            if change[0] == "Update":
                storage.update_folderhierarchy_change(change[1], curs)
            elif change[0] == "Delete":
                storage.delete_folderhierarchy_change(change[1], curs)
            elif change[0] == "Add":
                storage.insert_folderhierarchy_change(change[1], curs)
        conn.commit()
        conn.close()

    @staticmethod
    def get_folderhierarchy_folder_by_name(foldername, curs):
        sql = "SELECT * FROM FolderHierarchy WHERE DisplayName = '%s'" % foldername
        curs.execute(sql)
        folder_row = curs.fetchone()
        if folder_row:
            return folder_row
        else:
            return False

    @staticmethod
    def get_folderhierarchy_folder_by_id(server_id, curs):
        sql = "SELECT * FROM FolderHierarchy WHERE ServerId = '%s'" % server_id
        curs.execute(sql)
        folder_row = curs.fetchone()
        if folder_row:
            return folder_row
        else:
            return False

    @staticmethod
    def insert_item(table, calendar_dict, curs):
        server_id = calendar_dict["server_id"]
        del calendar_dict["server_id"]
        calendar_cols = ""
        calendar_vals = ""
        for calendar_field in list(calendar_dict.keys()):
            calendar_cols += (", '%s'" % calendar_field)
            calendar_vals += (", '%s'"  % repr(calendar_dict[calendar_field]).replace("'","''"))
        sql = "INSERT INTO %s ( 'ServerId' %s ) VALUES ( '%s' %s )" % (table, calendar_cols, server_id, calendar_vals)
        curs.execute(sql)

    @staticmethod
    def update_item(table, calendar_dict, curs):
        server_id = calendar_dict["server_id"]
        del calendar_dict["server_id"]
        calendar_sql = ""
        for calendar_field in list(calendar_dict.keys()):
            calendar_sql += (", %s='%s' "  % (calendar_field, repr(calendar_dict[calendar_field]).replace("'","''")))
        calendar_sql = calendar_sql.lstrip(", ")
        sql = "UPDATE %s SET %s WHERE ServerId='%s'" % (table, calendar_sql, server_id)
        curs.execute(sql)
    
    @staticmethod
    def delete_item(table, sever_id, curs):
        sql = "DELETE FROM %s WHERE ServerId='%s'" % (table, sever_id)
        curs.execute(sql)

    class ItemOps:
        Insert = 0
        Delete = 1
        Update = 2
        SoftDelete = 3

    class_to_table_dict = {
                        "Email" : "MSASEMAIL",
                        "Calendar" : "MSASCAL",
                        "Contacts" : "MSASCNTC",
                        "Tasks" : "MSASTASK",
                        "Notes" : "MSASNOTE",
                        "SMS" : "MSASMS",
                        "Document" : "MSASDOC"
                        }

    @staticmethod
    def item_operation(method, item_class, data, curs):
        if method == storage.ItemOps.Insert:
            storage.insert_item(storage.class_to_table_dict[item_class], data, curs)
        elif method == storage.ItemOps.Delete:
            storage.delete_item(storage.class_to_table_dict[item_class], data, curs)
        elif method == storage.ItemOps.Update:
            storage.update_item(storage.class_to_table_dict[item_class], data, curs)
        elif method == storage.ItemOps.SoftDelete:
            storage.delete_item(storage.class_to_table_dict[item_class], data, curs)

    @staticmethod
    def update_items(collections, path="pyas.asdb"):
        conn = sqlite3.connect(path)
        curs = conn.cursor()
        for collection in collections:
            for command in collection.Commands:
                if command[0] == "Add":
                    storage.item_operation(storage.ItemOps.Insert, command[1][1], command[1][0], curs)
                if command[0] == "Delete":
                    storage.item_operation(storage.ItemOps.Delete, command[1][1], command[1][0], curs)
                elif command[0] == "Change":
                    storage.item_operation(storage.ItemOps.Update, command[1][1], command[1][0], curs)
                elif command[0] == "SoftDelete":
                    storage.item_operation(storage.ItemOps.SoftDelete, command[1][1], command[1][0], curs)
            if collection.SyncKey > 1:
                storage.update_synckey(collection.SyncKey, collection.CollectionId, curs)
                conn.commit()
            else:
                conn.close()
                raise AttributeError("SyncKey incorrect")

        conn.commit()
        conn.close()

    @staticmethod
    def get_emails_by_collectionid(collectionid, curs):
        sql = "SELECT * from MSASEMAIL WHERE ServerId like '%s:%%'" % collectionid
        curs.execute(sql)
        return curs.fetchall()

    @staticmethod
    def update_synckey(synckey, collectionid, curs=None):
        cleanup = False
        if not curs:
            cleanup = True
            conn = sqlite3.connect("pyas.asdb")
            curs = conn.cursor()
        curs.execute("SELECT SyncKey FROM SyncKeys WHERE CollectionId = %s" % collectionid)
        prev_synckey = curs.fetchone()
        if not prev_synckey:
            curs.execute("INSERT INTO SyncKeys VALUES ('%s', '%s')" % (synckey, collectionid))
        else:
            curs.execute("UPDATE SyncKeys SET SyncKey='%s' WHERE CollectionId='%s'" % (synckey, collectionid)) 
        if cleanup:
            conn.commit()
            conn.close()

    @staticmethod
    def get_synckey(collectionid, path="pyas.asdb"):
        conn = sqlite3.connect(path)
        curs = conn.cursor()
        curs.execute("SELECT SyncKey FROM SyncKeys WHERE CollectionId = %s" % collectionid)
        try:
            synckey = curs.fetchone()[0]
        except TypeError:
            synckey = "0"
        conn.close()
        return synckey

    @staticmethod
    def create_db_if_none(path="pyas.asdb"):
        import os
        if not os.path.isfile(path):
            storage.create_db(path)

    @staticmethod
    def get_folder_name_to_id_dict(path="pyas.asdb"):
        conn = sqlite3.connect(path)
        curs = conn.cursor()
        curs.execute("SELECT DisplayName, ServerId FROM FolderHierarchy")
        id_name_list_of_tuples = curs.fetchall()
        name_id_dict = {}
        for id_name in id_name_list_of_tuples:
            name_id_dict.update({ id_name[0] : id_name[1] })
        conn.close()
        return name_id_dict

    @staticmethod
    def get_synckeys_dict(curs, path="pyas.asdb"):
        """
        This function returns a dictionary which its keys are the collection_ids and its
        values are the collections' synckey.
        for example:
        {'0': '1', '9': '696602903', '15': '1148150257', '2': '1893202198', '5': '1845584800', '12': '980326785'}
        :param curs: the storage curs
        :param path: the db path
        :return: dict of collection_id to sync_key
        :rtype: dict of str to str
        """
        conn = sqlite3.connect(path)
        curs = conn.cursor()
        curs.execute("SELECT * FROM SyncKeys")
        synckeys_rows = curs.fetchall()
        synckeys_dict = {}
        if synckeys_rows:
            if len(synckeys_rows) > 0:
                for synckey_row in synckeys_rows:
                    synckeys_dict.update({synckey_row[1]:synckey_row[0]})
        return synckeys_dict

    @staticmethod
    def get_new_mid(path="pyas.asdb"):
        pmid = int(storage.get_keyvalue("MID"))
        mid = str(pmid+1)
        storage.update_keyvalue("MID", mid)
        return mid

    @staticmethod
    def get_serverid_to_type_dict(path="pyas.asdb"):
        conn = sqlite3.connect(path)
        curs = conn.cursor()
        curs.execute("SELECT * FROM FolderHierarchy")
        folders_rows = curs.fetchall()
        conn.close()
        folders_dict = {}
        if folders_rows:
            if len(folders_rows) > 0:
                for folders_row in folders_rows:
                    folders_dict.update({folders_row[0]:folders_row[3]})
        else:
            raise LookupError("No folders found in FolderHierarchy table. Did you run a FolderSync yet?")
        return folders_dict