from xml.etree import ElementTree as ET
from xml.etree.ElementTree import Element
from xml.etree.ElementTree import SubElement
import time
import xmltodict


class BuildXml:

    def __init__(self, hd, bd):
        self.header_dict = hd
        self.body_dict = bd

    def main_header(self, d, parent, main_tag, tags):

        output_block = SubElement(parent, main_tag)

        for idx, tag in enumerate(tags):
            if tag == 'SourceCounter':
                source_counter = SubElement(output_block, 'SourceCounter')
                source_counter.text = '$$ReplaceSourceCounter$$'
            if tag == 'SourceID':
                source_id = SubElement(output_block, 'SourceID')
                source_id.text = '$$ReplaceSourceID$$'
            else:
                if tag == 'SourceCounter':
                    continue
                else:
                    ele = SubElement(output_block, tag)
                    ele.text = d[tags[idx]]

        return output_block

    def create_xml(self):

        xmlstring = '<ns7:BOLRequest xmlns:ns7="http://www.utilisoft.co.uk/namespaces/dccwb/UtilisoftBOL_1.0" ' \
                    'xmlns:ns2="http://www.dccinterface.co.uk/ServiceUserGateway" xmlns:ns3="' \
                    'http://www.w3.org/2000/09/xmldsig#" xmlns:ns4="http://www.dccinterface.co.uk/ResponseAndAlert" ' \
                    'xmlns:ns5="http://www.utilisoft.co.uk/namespaces/dccwb/UtilisoftProcessor_1.0" ' \
                    'xmlns:ns6="http://www.utilisoft.co.uk/namespaces/dccwb/UtilisoftInternal_1.0"></ns7:BOLRequest>'

        root = ET.fromstring(xmlstring)

        # Build Header Segment
        if 'PrecedingScenarioID' in self.header_dict:
            header = self.main_header(self.header_dict, root, 'Header',
                                      ('SourceID', 'SourceCounter', 'BusinessScenarioID', 'PrecedingScenarioID',))
        else:
            header = self.main_header(self.header_dict, root, 'Header',
                                  ('SourceID', 'SourceCounter', 'BusinessScenarioID',))

        # Build Body Segment
        body = SubElement(root, 'Body')
        body.append(ET.fromstring(xmltodict.unparse(self.body_dict, full_document=False)))

        return root
