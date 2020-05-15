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
import os


class ItemOps:
    Insert = 0
    Delete = 1
    Update = 2
    SoftDelete = 3


class storage:

    def __init__(self, path="pyas.asdb", force_create=False):
        """
        This function creates a new storage instance for the DB specified in the path variable.
        If the DB in the path variable does not exist - it creates it.
        With the force_create - create the db in path even if there is already a file there.
        :param path: The path of the DB
        :type path: str
        :param force_create: force the creation of a new db, even if there is already one.
        :type force_create: bool
        """
        self.conn = sqlite3.connect(path)
        self.curs = self.conn.cursor()

        if force_create or not os.path.isfile(path):
            self.create_db(path)

    def create_db(self, path):
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
        self.curs.execute(
            """CREATE TABLE FolderHierarchy (ServerId text, ParentId text, DisplayName text, Type text)""")
        self.curs.execute("""CREATE TABLE SyncKeys (SyncKey text, CollectionId text)""")
        self.curs.execute("""CREATE TABLE KeyValue (Key text, Value blob)""")

        self.curs.execute("""CREATE TABLE MSASEMAIL (ServerId text, 
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

        self.conn.commit()

        indicies = ['CREATE UNIQUE INDEX "main"."MSASEMAIL_ServerId_Idx" ON "MSASEMAIL" ("ServerId" ASC)',
                    'CREATE UNIQUE INDEX "main"."SyncKey_CollectionId_Idx" ON "SyncKeys" ("CollectionId" ASC)',
                    'CREATE UNIQUE INDEX "main"."KeyValue_Key_Idx" ON "KeyValue" ("Key" ASC)',
                    'CREATE UNIQUE INDEX "main"."FolderHierarchy_ServerId_Idx" ON "FolderHierarchy" ("ServerId" ASC)',
                    'CREATE  INDEX "main"."FolderHierarchy_ParentType_Idx" ON "FolderHierarchy" ("ParentId" ASC, "Type" ASC)',
                    ]
        for index in indicies:
            self.curs.execute(index)
        # storage.set_keyvalue("X-MS-PolicyKey", "0")   --- this is used for policy. part of the Provision command. i think it is not necessary
        # storage.set_keyvalue("EASPolicies", "")        --- this is used for policy. part of the Provision command. i think it is not necessary
        # storage.set_keyvalue("MID", "0")               --- mail id...? i think its used just for sending mails
        self.conn.commit()


    def set_keyvalue(self, key, value):
        self.curs.execute("INSERT INTO KeyValue VALUES ('%s', '%s')" % (key, value))
        self.conn.commit()

    def update_keyvalue(self, key, value):
        sql = "UPDATE KeyValue SET Value='%s' WHERE Key='%s'" % (value.replace("'","''"), key)
        self.curs.execute(sql)
        self.conn.commit()

    def get_keyvalue(self, key, path="pyas.asdb"):
        self.curs.execute("SELECT Value FROM KeyValue WHERE Key='%s'" % key)
        try:
            value = self.curs.fetchone()[0]
            return value
        except:
            return None


    def insert_folderhierarchy_change(self, folder):
        sql = """INSERT INTO FolderHierarchy VALUES ("%s", "%s", "%s", "%s")""" % (folder.ServerId, folder.ParentId, folder.DisplayName, folder.Type)
        self.curs.execute(sql)

    def update_folderhierarchy_change(self, folder):
        sql = "UPDATE FolderHierarchy SET ParentId='%s', DisplayName='%s', Type='%s' WHERE ServerId == '%s'""" % (folder.ParentId, folder.DisplayName, folder.Type, folder.ServerId)
        self.curs.execute(sql)


    def delete_folderhierarchy_change(self, folder):
        #Command only sent we permement delete is requested. Otherwise it would be 'Update' to ParentId='3' (Deleted Items).
        #sql = "UPDATE FolderHierarchy SET ParentId='4' WHERE ServerId == '%s'""" % (folder.ServerId)
        sql = "DELETE FROM MSASEMAIL WHERE ServerId like '%s:%%'" % (folder.ServerId)
        self.curs.execute(sql)
        sql = "DELETE FROM FolderHierarchy WHERE ServerId == '%s'" % (folder.ServerId)
        self.curs.execute(sql)

    def update_folderhierarchy(self, changes):
        """
        :param changes: list of tuples of command and objects.
                        for example:
                        [('ADD', <objects.MSASCMD.FolderHierarchy.Folder object>),
                        ('ADD', <objects.MSASCMD.FolderHierarchy.Folder object>)]
        :param path:
        :return:
        """
        for change in changes:
            if change[0] == "Update":
                self.update_folderhierarchy_change(change[1])
            elif change[0] == "Delete":
                self.delete_folderhierarchy_change(change[1])
            elif change[0] == "Add":
                self.insert_folderhierarchy_change(change[1])
        self.conn.commit()

    def get_folderhierarchy_folder_by_name(self, foldername):
        sql = "SELECT * FROM FolderHierarchy WHERE DisplayName = '%s'" % foldername
        self.curs.execute(sql)
        folder_row = self.curs.fetchone()
        if folder_row:
            return folder_row
        else:
            return False

    def get_folderhierarchy_folder_by_id(self, server_id):
        sql = "SELECT * FROM FolderHierarchy WHERE ServerId = '%s'" % server_id
        self.curs.execute(sql)
        folder_row = self.curs.fetchone()
        if folder_row:
            return folder_row
        else:
            return False

    def insert_item(self, table, calendar_dict):
        server_id = calendar_dict["server_id"]
        del calendar_dict["server_id"]
        calendar_cols = ""
        calendar_vals = ""
        for calendar_field in list(calendar_dict.keys()):
            calendar_cols += (", '%s'" % calendar_field)
            calendar_vals += (", '%s'"  % repr(calendar_dict[calendar_field]).replace("'","''"))
        sql = "INSERT INTO %s ( 'ServerId' %s ) VALUES ( '%s' %s )" % (table, calendar_cols, server_id, calendar_vals)
        self.curs.execute(sql)

    def update_item(self, table, calendar_dict):
        server_id = calendar_dict["server_id"]
        del calendar_dict["server_id"]
        calendar_sql = ""
        for calendar_field in list(calendar_dict.keys()):
            calendar_sql += (", %s='%s' "  % (calendar_field, repr(calendar_dict[calendar_field]).replace("'","''")))
        calendar_sql = calendar_sql.lstrip(", ")
        sql = "UPDATE %s SET %s WHERE ServerId='%s'" % (table, calendar_sql, server_id)
        self.curs.execute(sql)

    def delete_item(self, table, sever_id):
        sql = "DELETE FROM %s WHERE ServerId='%s'" % (table, sever_id)
        self.curs.execute(sql)

    def item_operation(self, method, item_class, data):
        class_to_table_dict = {
            "Email": "MSASEMAIL"
        }
        if method == ItemOps.Insert:
            self.insert_item(class_to_table_dict[item_class], data)
        elif method == ItemOps.Delete:
            self.delete_item(class_to_table_dict[item_class], data)
        elif method == ItemOps.Update:
            self.update_item(class_to_table_dict[item_class], data)
        elif method == ItemOps.SoftDelete:
            self.delete_item(class_to_table_dict[item_class], data)

    def update_items(self, collections):
        for collection in collections:
            for command in collection.Commands:
                if command[0] == "Add":
                    storage.item_operation(storage.ItemOps.Insert, command[1][1], command[1][0])
                if command[0] == "Delete":
                    storage.item_operation(storage.ItemOps.Delete, command[1][1], command[1][0])
                elif command[0] == "Change":
                    storage.item_operation(storage.ItemOps.Update, command[1][1], command[1][0])
                elif command[0] == "SoftDelete":
                    storage.item_operation(storage.ItemOps.SoftDelete, command[1][1], command[1][0])
            if collection.SyncKey > 1:
                storage.update_synckey(collection.SyncKey, collection.CollectionId)
                self.conn.commit()
            else:
                raise AttributeError("SyncKey incorrect")

        self.conn.commit()

    def get_emails_by_collectionid(self, collectionid):
        sql = "SELECT * from MSASEMAIL WHERE ServerId like '%s:%%'" % collectionid
        self.curs.execute(sql)
        return self.curs.fetchall()

    def update_synckey(self, synckey, collectionid):
        self.curs.execute("SELECT SyncKey FROM SyncKeys WHERE CollectionId = %s" % collectionid)
        prev_synckey = self.curs.fetchone()
        if not prev_synckey:
            self.curs.execute("INSERT INTO SyncKeys VALUES ('%s', '%s')" % (synckey, collectionid))
        else:
            self.curs.execute("UPDATE SyncKeys SET SyncKey='%s' WHERE CollectionId='%s'" % (synckey, collectionid))

    def get_synckey(self, collectionid):
        self.curs.execute("SELECT SyncKey FROM SyncKeys WHERE CollectionId = %s" % collectionid)
        try:
            synckey = self.curs.fetchone()[0]
        except TypeError:
            synckey = "0"
        return synckey

    def get_folder_name_to_id_dict(self):
        self.curs.execute("SELECT DisplayName, ServerId FROM FolderHierarchy")
        id_name_list_of_tuples = self.curs.fetchall()
        name_id_dict = {}
        for id_name in id_name_list_of_tuples:
            name_id_dict.update({ id_name[0] : id_name[1] })
        return name_id_dict

    @staticmethod
    def get_synckeys_dict(self):
        """
        This function returns a dictionary which its keys are the collection_ids and its
        values are the collections' synckey.
        for example:
        {'0': '1', '9': '696602903', '15': '1148150257', '2': '1893202198', '5': '1845584800', '12': '980326785'}
        :return: dict of collection_id to sync_key
        :rtype: dict of str to str
        """
        self.curs.execute("SELECT * FROM SyncKeys")
        synckeys_rows = self.curs.fetchall()
        synckeys_dict = {}
        if synckeys_rows:
            if len(synckeys_rows) > 0:
                for synckey_row in synckeys_rows:
                    synckeys_dict.update({synckey_row[1]:synckey_row[0]})
        return synckeys_dict


    def get_serverid_to_type_dict(self):
        self.curs.execute("SELECT * FROM FolderHierarchy")
        folders_rows = self.curs.fetchall()
        folders_dict = {}
        if folders_rows:
            if len(folders_rows) > 0:
                for folders_row in folders_rows:
                    folders_dict.update({folders_row[0]:folders_row[3]})
        else:
            raise LookupError("No folders found in FolderHierarchy table. Did you run a FolderSync yet?")
        return folders_dict



#   @staticmethod
#    def get_new_mid(path="pyas.asdb"):
#        pmid = int(storage.get_keyvalue("MID"))
#        mid = str(pmid+1)
#        storage.update_keyvalue("MID", mid)
#        return mid

#    @staticmethod
#   def close_conn_curs(conn):
#      try:
#         conn.close()
#    except:
#       return False
#  return True
