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

"""[MS-ASDTYPE] MS-AS data types objects"""

"""
Exchange ActiveSync: Data Types

Specifies the Exchange ActiveSync data types that are used by the Exchange ActiveSync Protocol XML schema definitions 
(XSDs).

link - https://docs.microsoft.com/en-us/openspecs/exchange_server_protocols/ms-asdtype/dcfe20e1-cb36-457f-8c7b-e5c61351f7d3
"""

class datatype_TimeZone:
    def get_local_timezone_bytes():
        #TODO
        return
    def get_timezone_bytes(timezone):
        #TODO
        return
    class Timezones:
        GMT = 0
        #TODO

class datatype_String:
    pass
    #TODO - handle byte array, email, telephone, timezone and compact Datetime