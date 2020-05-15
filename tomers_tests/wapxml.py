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


class wapxmltree(object):
    """
    This class represents a textual XML tree.
    It has a simple xml header, and a root node (of type wapxmlnode) which holds all the other nodes as children.
    """
    def __init__(self, root_node=None, xmlns=None):
        """
        constructs a new XML tree. If root_node and xmlns provided - initializes the root node of the tree.
        :param root_node: The root node of the tree. This node can be modified to include child nodes in two ways:
            1. using the add_children method
            2. creating new nodes, and setting this node as their parent node
        :type root_node: wapxmlnode
        :param xmlns: the xml namespace of the node #TODO - and its children..?
        :type xmlns: str
        :return: None
        """
        self.header = "<?xml version=\"1.0\" encoding=\"utf-8\"?>"

        if root_node and xmlns:
            self.set_root(root_node, xmlns)
        else:
            self._root_node = None

    def set_root(self, root_node, xmlns):
        """
        sets the root node of the tree. The root node holds all the other nodes as children.
        :param root_node: The root node of the tree. This node can be modified to include child nodes in two ways:
            1. using the add_children method
            2. creating new nodes, and setting this node as their parent node
        :type root_node: wapxmlnode
        :param xmlns: the xml namespace of the node #TODO - and its children..?
        :type xmlns: str
        :return: None
        """
        self._root_node = root_node
        self._root_node.set_root(True, xmlns, self)

    def get_root(self):
        """
        returns the root node of the tree
        :return: The root node
        :rtype: wapxmlnode
        """
        return self._root_node

    def __repr__(self):
        """
        this function returns the tree representation - which is the header + the representation of the root
        node (recursivly calling the representation of all the other nodes)
        :return: the tree representation
        """
        if self._root_node:
            return self.header + repr(self._root_node)


class wapxmlnode(object):
    """
    This class represents a node in an XML textual tree.
    The node is related to the tree by its parent(s) - all the way up to the root node of the tree
    """
    def __init__(self, tag, parent=None, text=None, cdata=None):
        """
        constructs a new wapxmlnode instance
        :param tag: The tag of the node
        :param parent: The parent of the node. If not None - this node will be added to the children list of the
                        parent Node
        :param text: The text of the node
        :param cdata: #TODO - what is dis?
        """
        self.tag = tag
        self.text = text
        self.cdata = cdata
        self._children = []
        self._is_root = None
        self._xmlns = None
        self._parent = None
        if parent:
            try:
                self.set_parent(parent)
            except Exception as e:
                print(e)

    def set_parent(self, parent):
        """
        This function sets the node's parent and adds itself to the parent's children list
        :param parent: The parent node
        :return: None
        """
        parent.add_child(self)
        self._parent = parent

    def get_parent(self):
        return self._parent

    def add_child(self, child):
        self._children.append(child)

    def remove_child(self, child):
        self._children.remove(child)

    def set_root(self, true_or_false, xmlns=None, parent=None):
        self._is_root = true_or_false
        self._xmlns = xmlns
        self._parent = parent

    def is_root(self):
        """
        True if the node set to root, false if not
        :return: True if root node
        """
        return self._is_root

    def set_xmlns(self, xmlns):
        self._xmlns = xmlns

    def get_xmlns(self):
        return self._xmlns

    def has_children(self):
        if len(self._children) > 0:
            return True
        else:
            return False

    def get_children(self):
        return self._children

    def __repr__(self, tabs="  "):
        """
        This function returns a string representation of the node and its children, seperated by tabs, by iterating them and calling
        their __repr__ method
        :param tabs: the seperator between nodes and its children's representation
        :return: The string representation of the node and it's children
        """
        if (self.text != None) or (self.cdata != None) or (len(self._children)>0):
            inner_text = ""
            if self.text != None:
                inner_text+=str(self.text)
            if self.cdata != None:
                inner_text+= "<![CDATA[%s]]>" % str(self.cdata)
            if self.has_children():
                for child in self._children:
                    inner_text+=child.__repr__(tabs+"  ")
            if not self._is_root:
                end_tabs = ""
                if self.has_children(): end_tabs = "\r\n"+tabs
                return "\r\n%s<%s>%s%s</%s>" % (tabs, self.tag, inner_text, end_tabs, self.tag)
            else: return  "\r\n<%s xmlns=\"%s:\">%s\r\n</%s>" % (self.tag, self._xmlns, inner_text, self.tag)
        elif self._is_root:
            return "\r\n<%s xmlns=\"%s:\"></%s>" % (self.tag, self._xmlns, self.tag)
        else:
            return "%s<%s />" % (tabs, self.tag)

    def __iter__(self):
        if len(self._children) > 0:
            for child in self._children:
                yield child

    def __str__(self):
        return self.__repr__()
                    




