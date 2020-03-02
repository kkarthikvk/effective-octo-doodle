import nltk
import re

relevant_terms = set(['phone', 'call', 'phones', 'skype'])

# in person and phone given in the same line at times.
# in person follow by phone -> so it refers to interviews
relevant_terms2 = set(['in person', 'interview', 'interviews', 
                       'screen', 'manager', 'hm', 'pm',
                      'on site', 'onsite', 'screening'])

multi_expression_tokens = list(relevant_terms) + list(relevant_terms2)

mwetokenizer = nltk.MWETokenizer(separator=' ')

for word in multi_expression_tokens:
    mwetokenizer.add_mwe(word.split())

def approach1(sentence):
    lower_sentence = sentence.lower()
    tokens = nltk.regexp_tokenize(lower_sentence, r'\w+')
    tokens = mwetokenizer.tokenize(tokens)
    set_tokens = set(tokens)
    
    if relevant_terms.intersection(set_tokens) and \
       relevant_terms2.intersection(set_tokens):
        return True
    else:
        return False
    

def sentence_block_to_result(sentence_block):
    sans_space = re.sub('\s', '', sentence_block)
    if len(sans_space) == 0:
	print("not found")
        return "Not Found"
    else:
        return "Yes"

def feeder(filepath, text_content):
    related_sentences = []
    for individual_sentences in text_content.split('\n'):
        if approach1(individual_sentences): 
            related_sentences.append(individual_sentences)

    related_sentences_block = "\n".join(related_sentences)
    result = sentence_block_to_result(related_sentences_block)
    return [{
        'sentence_block': related_sentences_block, 
        'inference'     : result
    }]
