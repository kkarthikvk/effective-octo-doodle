import re
import pandas as pd
pd.options.mode.chained_assignment = None
import time
from collections import defaultdict

# Import for db credentials
from sqlalchemy import create_engine
from decouple import Config, RepositoryEnv
#DOTENV_FILE = '/home/roy/Desktop/Req_Intelligence/.env'
DOTENV_FILE = '../.env'
env_config = Config(RepositoryEnv(DOTENV_FILE))

# Import custom packages
from .. import helpers
from req_intl.modules.extraction import business_skills_overview as business_skills_overview


def createEngine(user, pwd, host, db):
    ''' create database connection '''
    user = user
    pwd = pwd
    host = host
    db = db
    engine = create_engine('postgresql://'+user+':'+pwd+'@'+host+'/'+db)
    return engine


def fetchDeptTable(client_name = 'Best Buy'):
    schema = env_config.get('DEV_DB_SCHEMA_PUBLIC')

    # Create Db engine
    engine = createEngine(user=env_config.get('DEV_DB_USER'),
                            pwd=env_config.get('DEV_DB_PASSWORD'),
                            host=env_config.get('DEV_DB_HOST'),
                            db=env_config.get('DEV_DB_NAME'))

    # Create query for resume
    
    query1 = """
        select
        client_id
        from
        """+schema+""".truejd_client
        where client_name = '""" + client_name + """'
        """
            
    client_id = pd.read_sql_query(query1, con = engine)
    
    try:
        client_id = client_id.iloc[0,0]
    except Exception:
        client_id = 1
    
    query2 = """
        select
        dept_id,
        dept_name,
        master_flag,
        merged_dept_id,
        dept_keywords
        from
        """+schema+""".truejd_department where client_id_fk_id = """ + str(client_id)
    
    df_dept = pd.read_sql_query(query2, con = engine)
    
    return client_id, df_dept




def get_department_block_from_jd(text_content):

    ''' this function gets department block from jd. We all the reusable function business_skills_overview'''
    dept_block = ''
    project_block = ''
    
    start_time = time.time()
    all_blocks = business_skills_overview.business_skills_overview('', text_content)
    end_time = time.time()
    print("Execution time for business_skills_overview : "+ str(end_time - start_time))
    
    dept_block = all_blocks[1][1]  # Department block
    
    if dept_block != '':       
        return dept_block
    else:
        project_block = all_blocks[2][1] + ' ' + all_blocks[0][1]  # Adds Project Description and Business Overview 
        return project_block        
        
 

           
def get_department_from_file(file_path):

    ''' This function extracts dept from filename(if mentioned ) '''
    
    # Change delimiters of special characters
    f = re.sub(r'[^a-zA-Z0-9 ]', '||', file_path)
    f = re.sub(r'[||]{1,}', '||', f)
    f = f.split('||')
    
    # Use tag words to find matches
    tag_words = ['team', 'department', 'dept', 'teams']
    
    # Look for tag words
    dept_from_file = ''
    for idi, i in enumerate(f):
        if any(t in i.lower() for t in tag_words):
            dept_from_file = i  
    
    # Check if only word left is tag_words
    if any(dept_from_file.lower().strip() == t.lower().strip() for t in tag_words):         
        return ''        
    else:        
        return dept_from_file


def get_department_NER(txt_block, client_name):
    
    # Include list
    # catchwords_clients = {
    #     'Best Buy'           :   ['team', 'teams' , 'center', 'engineering', 'department', 'services'],
    #     'State of Minnesota' :   ['center', 'team', 'engineering', 'department', 'services'],
    #     'AT&T'               :   ['center', 'team', 'engineering', 'department', 'services']
    #     }

    """ Create a similar dict from helpers client reference dictionary """
    catchwords_clients ={}

    if client_name == "Unknown":
        dict_from_helpers = helpers.client_unknown_reference
    else:
        dict_from_helpers = helpers.client_reference

    for each_key in dict_from_helpers.keys():
        catchwords_clients[each_key] = dict_from_helpers[each_key]['catchwords_clients']

    # print(catchwords_clients)

    try:
        catchwords = catchwords_clients[client_name]
    except Exception:
        catchwords = ['team', 'teams' , 'center', 'engineering', 'department', 'services']


    # Need capitalization of catchwords tags
    for i in catchwords:
        if i in txt_block:
            txt_block = txt_block.replace(i, i.title())
     
    # Words we do not want to be identified as department. Make it lower case
    # no_dept_tags_clients = {
    #     'Best Buy'           :   ['Employee Hub'],
    #     'State of Minnesota' :   ['Employee Hub'],
    #     'AT&T'               :   ['Budget Management']
    #     }



    """ Create a similar dict from helpers client reference dictionary """
    no_dept_tags_clients ={}
    for each_key in dict_from_helpers.keys():
        no_dept_tags_clients[each_key] = dict_from_helpers[each_key]['no_dept_tags_clients']

    # print(no_dept_tags_clients)  


    try:
        no_dept_tags = no_dept_tags_clients[client_name]
    except Exception:
        no_dept_tags = []

    for i in no_dept_tags:
        if i in txt_block:
            txt_block = txt_block.replace(i, i.lower())  
        
    # Special characters is not getting identified
    txt_block = txt_block.replace('/', ' And ')    
        
    # Lot of issues with undetected tags. Below are the most commonly occuring words
    # issuetags_clients = {
    #     'Best Buy'           :   ['content', 'domain', 'Content', 'Domain', 'Domains', 'Affairs'],
    #     'State of Minnesota' :   [],
    #     'AT&T'               :   ['content', 'domain', 'Content', 'Domain', 'Domains', 'Affairs']
    #     }
    
    """ Create a similar dict from helpers client reference dictionary """
    issuetags_clients ={}
    for each_key in dict_from_helpers.keys():
        issuetags_clients[each_key] = dict_from_helpers[each_key]['issuetags_clients']

    # print(issuetags_clients)

    try:
        issuetags = issuetags_clients[client_name]
    except Exception:
        issuetags = ['content', 'domain', 'Content', 'Domain', 'Domains', 'Affairs']
    for i in issuetags:
        txt_block = txt_block.replace(i, 'Organization ' + i.title())    # Adding Organization. Very crude.
    
    # Run spacy NER model    
    entities = []
    #doc = helpers.spacy_get_ner_labels_en_core_web_sm(txt_block)
    doc = helpers.spacy_get_ner_labels_en_core_web_lg(txt_block)
    for ent in doc:
        entities.append((ent['label'], ent['string']))

    # Collect entities in a dictionary and then filter ORG
    ent_by_lbl = defaultdict(list)
    for lbl, t in entities:
        ent_by_lbl[lbl].append(t)
    
    extractedDept = []    
    extractedDept = ent_by_lbl['ORG']   
        
#    print("NER department ::::::::::::::::::::::::::::::::")
#    print(extractedDept)    
    
    if len(extractedDept) == 0:
        return ''
    else:        
#        for idx, i in enumerate(extractedDept):   
#            if 'Organization' in i:
#                extractedDept[idx] = i.replace('Organization', '')
        extractedDept = [re.sub('Organization', '', itm.strip()) for itm in extractedDept]        
        # Removing one word department which is unlikely. Domain term will get picked up.
        # If there is any such 1 word department like UGC, we expect to pick up in next approach.
        extractedDept = [i for i in extractedDept if len(i.split()) > 1]
        
        for idx, i in enumerate(extractedDept):   
            if 'team' not in i.lower():                
                extractedDept[idx] = i+' Team'
                
        # Remove multi space to single
        extractedDept = [re.sub(' +', ' ', itm.strip()) for itm in extractedDept]        
                  
#        print("Cleaned NER department ::::::::::::::::::::::::::::::::")
#        print(extractedDept)
        if len(extractedDept) > 0:
            # Pass first department
            return extractedDept[0]  
        else:
            return '' 


def clean_dept_db_words(w):    
    drop_words = ['the', 'teams', 'team', 'department']
    for i in drop_words:
        w = re.sub(i,'',w.lower())
    return w.strip().lower()    


def check_existing_merged_db_dept(df, found_dept): 
    found_dept = clean_dept_db_words(found_dept)
    df['dept_name_clean'] = df['dept_name'].apply(clean_dept_db_words)
    
    '''  Check if ner value is present in database or not''' 
    merged_id = -1
    if len(df) == 0 or found_dept == '':
        return ''
    elif len(df) > 0 and found_dept != '':
        for i in range(len(df)):
            if df.loc[i, 'dept_name_clean'] == found_dept and df.loc[i, 'master_flag'] == False:
                merged_id = df.loc[i, 'merged_dept_id']          
    if merged_id == -1 or str(merged_id) == 'nan' or merged_id == None:
        return ''
    else:
        val = df[df.dept_id == merged_id]['dept_name']
        return val.values[0]        
           
            

def convert_clean_dept_kewwords(txt):
    ''' Clean pipe delimited keywords to a sentence of unique words. This will help to find intersection with department block.'''
    #txt = 'domain|domains|domain services|domain team'
    txt_lst = txt.split('|')
    txt_lst = ' '.join(txt_lst)
    txt_lst = re.sub(r'[^a-zA-Z0-9]', r' ', txt_lst)
    txt_lst = clean_dept_db_words(txt_lst)
    txt_lst = re.sub(' +',' ',txt_lst)
    txt_lst = list(set(txt_lst.split()))
    txt_lst = [itm.lower().strip() for itm in txt_lst]
    return ' '.join(txt_lst)


def get_department_word_similarity(txt, df):
    ''' keyword match to find mastered records '''
    txt = re.sub(r'[^a-zA-Z]', r' ', txt)
    txt = clean_dept_db_words(txt)
    txt = re.sub(' +',' ',txt)
    
    df = df[df.master_flag == True]

    if len(df) == 0 or txt == '':
        return ''
    
    elif len(df) > 0 and txt != '':
        txt = txt.lower().split()
        txt = [itm.lower().strip() for itm in txt]
        
        df['dept_unique_tokens'] = df['dept_keywords'].apply(convert_clean_dept_kewwords)        
        df['score'] = df['dept_unique_tokens'].apply(lambda x: len(set(txt).intersection(set(x.split()))))
        df = df.sort_values(by=['score'], ascending=False).reset_index(drop=True) 
        
        return df.loc[0, 'dept_name']
    else:
        return ''
    


def feeder(file_path, text_content, client_name = 'Unknown'):
    
    ''' Retreive db tables. We need client id and department table '''
    client_id, df_dept = fetchDeptTable(client_name)

    # if client_name == "Unknown":
        # client_name = "Best Buy"

     
    ''' Check file name for department'''
    dept_from_filename = get_department_from_file(file_path) 

    if dept_from_filename != '':
        print("Department returned from filename.")
        return [{"inference" : dept_from_filename}]

    else:
        
        ''' If not found from file ,trying to get department name from NER and keywords check approach ''' 
        department_block_wo_header = ''
        department_block_wo_header = get_department_block_from_jd(text_content)
        
        ''' Call NER Approach. All text will be passed '''
        
        #department_block_wo_header = 'We are part of Product Domain Teams, building and delivering software and systems that supplies product data for Best Buy eCommerce and enterprise use.'
        #department_block_wo_header = 'We are the Best Buy Product Data Management (PDM) team, building and delivering software and systems that ingest, manage, and publish product data for Best Buy eCommerce and enterprise use. We are committed to delivering the right software at the right time, using automated testing and agile management practices to achieve those goals. This role will have 100% pair programming.'
        
        dept_from_ner = ''
        dept_from_ner = get_department_NER(department_block_wo_header, client_name)
        dept_from_ner = helpers.case_formatting(dept_from_ner)
        print("Department from NER: " + str(dept_from_ner))
        ''' What if ner department is already present in db ????? '''
        map_dept_from_ner = check_existing_merged_db_dept(df_dept, dept_from_ner.lower())
        if map_dept_from_ner != '':
            dept_from_ner = map_dept_from_ner
        print("Department from NER after DB Match: " + str(dept_from_ner))
        
        ''' Call Keyword Search Approach. Only department block will be sent as input '''
        dept_from_keywords_search = ''
        if department_block_wo_header != '':
            dept_from_keywords_search = get_department_word_similarity(department_block_wo_header, df_dept)  #calling keyword check approach
            dept_from_keywords_search = helpers.case_formatting(dept_from_keywords_search)
        print("Department from keyword search: " + str(dept_from_keywords_search))    
        
        ''' Return values '''
        if dept_from_ner != '' and dept_from_keywords_search != '':
            return [{"inference": dept_from_ner}, {"inference": dept_from_keywords_search}]
        elif dept_from_ner != '' and dept_from_keywords_search == '':
            return [{"inference": dept_from_ner}, {"inference": ''}]
        elif dept_from_ner == '' and dept_from_keywords_search != '':
            return [{"inference": dept_from_keywords_search}, {"inference": ''}]
        else:
            return [{"inference": ''}]
        