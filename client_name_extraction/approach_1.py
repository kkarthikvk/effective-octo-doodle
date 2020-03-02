import os
import operator
from .. import helpers

# bby_tags = ['bby', 'allegis cls', 'ags cls', 'allegis', 'best buy', 'geek squad', 'ags', 'bestbuy']
# som_tags = ['som ' , 'state of minnesota']
# optum_tags = ['optum']
# target_tags = ['tgt-3brdg', 'tgt']
# carribou_tags = ['carribou']
# att_tags = ['att', 'atnt', 'at&t', 'at & t']


def extraction(filename, text_content, tag_list):
    count = 0
    # Check for filename
    if any(tag in filename.lower() for tag in tag_list):
        return 100
    else:
        for tag in tag_list:
            if tag in text_content.lower():
                count += 1
        return count


def feeder(file_path, text_content):
    file_name = os.path.split(file_path)
    name_without_extension = os.path.splitext(file_name[-1])[0]

    # client_map = {
    #         'Best Buy'          :           extraction(name_without_extension, text_content, bby_tags),
    #         'State of Minnesota':           extraction(name_without_extension, text_content, som_tags),
    #         'Optum'             :           extraction(name_without_extension, text_content, optum_tags),
    #         'Target'            :           extraction(name_without_extension, text_content, target_tags),
    #         'Caribou Coffee'    :           extraction(name_without_extension, text_content, carribou_tags),
    #         'AT&T'              :           extraction(name_without_extension, text_content, att_tags)
    #         }
    # print(client_map)

    """ Create the dictionary similar to the above dict from the client reference dict """

    client_map = {}
    dict_from_helpers = helpers.client_reference
    for each_key in dict_from_helpers.keys():
        # print(dict_from_helpers[each_key]['name_tags'])
        client_map[each_key] = extraction(name_without_extension,text_content,dict_from_helpers[each_key]['name_tags'])
    # print(client_map)


#    print('client ***********************************************************')
#    print(client_map)
    if max(list(client_map.values())) != 0:
        client = max(client_map.items(), key=operator.itemgetter(1))[0]
        return [{"inference": client}]
    else:
        return [{"inference": 'Unknown'}]
