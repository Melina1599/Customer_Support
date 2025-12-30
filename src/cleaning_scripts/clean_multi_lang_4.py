import pandas as pd

def clean_multi_lang_4(filepath):
    #ver colab
    #https://colab.research.google.com/drive/1SCWVtMKoc_IUISPXgLUThPH1d5sijgeh#scrollTo=Pe-WDL8rMTkF


    df = pd.read_csv(filepath)


    #---------------------------     1. Limpieza estandar    -------------------------------
    #Identificar columnas de texto y categorías
    cols_to_fix = [column for column in df.columns]
    
    for column in cols_to_fix:
        if df[column].dtype == 'object':  #Solo si es tipo texto
            df[column] = (df[column]
                       .str.lower()       #Todo a minúsculas
                       .str.strip()       #Quitar espacios al inicio/final
                       .str.replace(r'\s+', ' ', regex=True)) #Quitar espacios dobles
    

    df = df.dropna(subset=['body'])
    
    values_to_fill = {
        'subject': 'no matter',
        'answer': 'unanswered'
    }
    df = df.fillna(value=values_to_fill)

    tag_columns = [col for col in df.columns if col.startswith('tag_')]
    df[tag_columns] = df[tag_columns].fillna('not provided')



    #---------------------      2. Mapeo de typos y negocio     ---------------------
    expected_types = ['incident', 'request', 'problem','change']
    df = df[df['type'].isin(expected_types)]

    type_rename = {'type': 'ticket_type'}
    df = df.rename(columns=type_rename)


    split_data = df['queue'].str.split('/', n=1, expand=True)
    df['sector'] = split_data[0].str.strip()
    if split_data.shape[1] > 1:
        df['specific_sector'] = split_data[1].fillna("not provided").str.strip()
    else:
        df['specific_sector'] = "not provided"



    #----------------------     3. Transformacion       --------------------------
    priority_map = {
        'low': 2,
        'medium': 3,
        'high': 4
    }
    df['priority_level'] = df['priority'].map(priority_map)

    lang_map = {'en': 'english', 'de': 'german'}
    df['language'] = df['language'].replace(lang_map)
    

    #-------------------- A. TAG 1
    mask = df['tag_1'].astype(str).str.contains(',', na=False)

    def distribuir_tags(row):
        tags = [t.strip().lower() for t in str(row['tag_1']).split(',')]        
        row['tag_1'] = tags[0]
        
        for i, tag in enumerate(tags[1:], start=2):
            col_name = f'tag_{i}'
            if col_name in df.columns:
                row[col_name] = tag                
        return row
    if mask.any():
        df.loc[mask] = df.loc[mask].apply(distribuir_tags, axis=1)

    group_tag1 = {
        'technical support': ['technical', 'it', 'tech support', 'technical support', 'hardware', 'infrastructure', 'system failure', 'server', 'device', 'integration', 'api', 'synchronization', 'maintenance', 'update', 'upgrade'],
        'security': ['security', 'cybersecurity', 'cyber-sicherheit', 'sicherheit', 'secure', 'breach', 'data leak', 'datenverletzung', 'datenverstoß', 'datensicherheit', 'malware', 'virus', 'encryption', 'privacy', 'access'],
        'bugs & performance': ['bug', 'crash', 'performance', 'slow', 'issue', 'disruption', 'outage', 'un outage', 'network'],
        'product & feedback': ['feature', 'features', 'feedback', 'product', 'website', 'gaming', 'documentation', 'guidance', 'training'],
        'sales & marketing': ['sales', 'marketing', 'campaign', 'campaigns', 'digital campaign', 'digital marketing', 'digitalmarketing', 'socialmedia', 'social media', 'social', 'ad', 'lead', 'brand', 'brand growth', 'growth', 'strategy', 'digital strategy', 'digital'],
        'billing & payments': ['billing', 'payment', 'pricing', 'refund', 'returns', 'return', 'replacement'],
        'customer account': ['customer', 'account', 'login', 'onboarding', 'support', 'inquiry', 'consultation', 'order', 'shipment'],
        'data management': ['data', 'data loss', 'datenverlust', 'critical data loss', 'recovery', 'backup'],
        'corporate': ['hr', 'employee', 'business', 'compliance', 'investment', 'medical', 'project'],
        'incident': ['incident', 'urgent', 'critical', 'disappearance', 'unfall'],
        'general': ['general', 'communication', 'notification', 'efficiencyloss']
    }
    map_tag_1 = {val: key for key, values in group_tag1.items() for val in values}
    df['tag_1'] = df['tag_1'].map(map_tag_1).fillna(df['tag_1'])
    
    
    #--------------------- B. TAG 2
    group_tag2 = {
        'technical support': ['it', 'technical', 'hardware', 'tech support', 'software', 'tech', 'maintenance', 'server', 'database', 'update', 'upgrade', 'system', 'application', 'api', 'integration', 'integrationsschwierigkeit', 'troubleshooting', 'infrastructure', 'installation', 'setup', 'sync', 'synchronization', 'syncing', 'connectivity', 'connection', 'disconnection', 'router', 'firewall', 'antivirus', 'docker', 'elasticsearch', 'mongodb', 'firebase', 'sap', 'jira', 'hubspot', 'crm', 'automation', 'plugin', 'tools', 'tool', 'equipment', 'printer', 'scanner', 'projection', 'display', 'audio', 'vr', 'smart devices', 'mobile', 'desktop', 'saas', 'platform', 'implementation', 'customization'],
        'security': ['security', 'breach', 'security breach', 'virus', 'malware', 'encryption', 'privacy', 'protection', 'data protection', 'vulnerability', 'authentication', 'access', 'compliance', 'hipaa', 'safety'],
        'bugs & performance': ['performance', 'bug', 'network', 'disruption', 'outage', 'crash', 'issue', 'alert', 'error', 'glitch', 'delay', 'optimization', 'compatibility', 'internet', 'accuracy'],
        'product feedback': ['product', 'feedback', 'feature', 'documentation', 'guidance', 'training', 'website', 'dashboard', 'analytics', 'data_analysis', 'dataanalysis', 'data analytics', 'dataanalytics', 'analysis', 'metrics', 'metric', 'reporting', 'improvement', 'enhancement', 'specifications', 'model', 'web', 'app', 'live stream', 'live streaming'],
        'sales & marketing': ['sales', 'marketing', 'strategy', 'campaign', 'campaigns', 'socialmedia', 'social media', 'social', 'digital', 'digitalmarketing', 'digital campaign', 'digital strategy', 'lead', 'ad', 'advertising', 'adtargeting', 'branding', 'brand growth', 'growth', 'engagement', 'ecommerce'],
        'billing & payments': ['payment', 'billing', 'refund', 'pricing', 'price', 'invoice', 'charge', 'subscription', 'abonnement', 'finance', 'financial', 'accounting', 'reconciliation', 'investment', 'portfolio', 'forecasting', 'forecast', 'inventory'],
        'customer account': ['customer', 'account', 'login', 'support', 'assistance', 'inquiry', 'onboarding', 'shipment', 'order', 'return', 'replacement', 'service', 'request', 'follow-up'],
        'data management': ['data', 'data loss', 'dataloss', 'backup', 'recovery', 'storage', 'data transfer'],
        'corporate': ['medical', 'healthcare', 'hospital', 'patient', 'medicaldevices', 'hr', 'employee', 'recruitment', 'interview', 'project', 'projectmanagement', 'collaboration', 'alignment', 'business'],
        'general': ['general', 'communication', 'notification'],
        'incident': ['incident', 'investigation']
    }
    map_tag_2 = {val: key for key, values in group_tag2.items() for val in values}
    df['tag_2'] = df['tag_2'].map(map_tag_2).fillna(df['tag_2'])


    #--------------------- C. TAG 3
    group_tag3 = {
        'technical support': [
            'it', 'tech support', 'software', 'technical', 'hardware', 'maintenance', 'integration', 'synchronization', 'api', 'sync', 'syncing', 'connectivity', 'connection', 'server', 'database', 'update', 'upgrade', 
            'configuration', 'troubleshooting', 'implementation', 'automation', 'deployment', 'infrastructure', 'tool', 'plugin', 'driver', 'magento', 'sap', 'jira', 'docker', 'elasticsearch', 'quickbooks', 'xero', 
            'saas', 'platform', 'macbook', 'nzxt', 'device', 'browser', 'outdated software', 'outdated', 'softwareversion', 'application', 'app', 'online', 'scanner', 'ubuntu', 'hubspot', 'shopware', 'crm', 'tools',
            'softwareintegration', 'kubernetes', 'upload', 'system', 'postgresql', 'raid-controller', 'desktop', 'mobile', 'multipleplatforms', 'outdateddependencies', 'mongodb', 'software compatibility', 
            'technology', 'adobe', 'software-as-a-service', 'terraform', 'code', 'adobesign', 'saperp', 'wordpress', 'node.js', 'setup', 'router', 'nuendo', 'salesforce', 'sap erp', 'technical support', 'smartsheet', 
            'videomeetings', 'firewall', 'outdated firewall', 'outdated plugin', 'antivirus', 'gaming', 'serverresource', 'macos', 'smartdevices', 'multiple', 'oracle', 'system specifications', 
            'systemrequirements', 'system requirement'
        ],
        'security': [
            'breach', 'security', 'virus', 'malware', 'encryption', 'vulnerability', 'authentication', 'access', 'accesscontrol', 'protection', 'privacy', 'policy', 'hipaa', 'cybersecurity', 'password', 'dataprotection', 
            'unauthorized', 'integrity', 'confidentiality', 'security compliance', 'data protection', 'dataprivacy', 'permissions', 'permission', 'safety', 'measures'
        ],
        'bugs & performance': [
            'performance', 'bug', 'outage', 'network', 'crash', 'disruption', 'issue', 'alert', 'error', 'problem', 'slowdown', 'delay', 'optimization', 'discrepancy', 'disparity', 'conflict', 'correction', 'adjustment', 
            'traffic', 'overload', 'protocol', 'glitch', 'failure', 'malfunction', 'stability', 'delays', 'crashing', 'load speed', 'incompatibility', 'discrepancies', 'concurrent', 'trouble', 'compatibility'
        ],
        'product feedback': [
            'documentation', 'feedback', 'feature', 'product', 'guidance', 'training', 'analytics', 'dataanalytics', 'data analytics', 'analysis', 'metrics', 'metric', 'dashboard', 'reporting', 'improvement', 'enhancement', 
            'customization', 'recommendation', 'content', 'website', 'onlinepresence', 'algorithm', 'workflow', 'requirement', 'dataanalysis', 'design', 'guidelines', 'document', 'precision', 'user interface', 
            'system specifications', 'model', 'report', 'option', 'specifications', 'specification', 'flexibility', 'informationstreams', 'accessibility', 'scaling', 'live-stream', 'live stream'
        ],
        'sales & marketing': [
            'marketing', 'sales', 'strategy', 'lead', 'campaign', 'promotion', 'engagement', 'digital', 'digitalmarketing', 'digital marketing', 'digitalstrategy', 'branding', 'brand', 'brandgrowth', 
            'growth', 'expansion', 'targeting', 'socialmedia', 'e-commerce', 'launch', 'online visibility', 'contentstrategy', 'digitalcampaigns', 'digitalcampaign', 'digital strategy', 'digital services', 
            'digitalsignage', 'social media marketing', 'digital marketing services', 'campaignparticipation', 'social media', 'ad', 'offer', 'tracking', 'competition'
        ],
        'billing & payments': ['refund', 'payment', 'subscription', 'pricing', 'billing', 'discount', 'invoice', 'charge', 'fee']
    }
    map_tag_3 = {val: key for key, values in group_tag3.items() for val in values}
    df['tag_3'] = df['tag_3'].map(map_tag_3).fillna(df['tag_3'])
    
    #--------------------- D. tag 4
    mask = df['tag_4'].str.contains(',', na=False)

    def redistribuir_tags(row):
        tags = [t.strip() for t in str(row['tag_4']).split(',')]    
        row['tag_4'] = tags[0]
        
        otras_cols = ['tag_5', 'tag_6', 'tag_7', 'tag_8']
        for i, extra_tag in enumerate(tags[1:]):
            if i < len(otras_cols):
                if pd.isna(row[otras_cols[i]]) or row[otras_cols[i]] == 'none provided':
                    row[otras_cols[i]] = extra_tag
        return row
    df.loc[mask] = df.loc[mask].apply(redistribuir_tags, axis=1)

    
    map_tag_4 = {
        # technical support
        'it': 'technical support', 'tech support': 'technical support', 'software': 'technical support', 'hardware': 'technical support', 'integration': 'technical support', 'api': 'technical support', 'database': 'technical support', 'server': 'technical support', 'technical': 'technical support', 'update': 'technical support', 'upgrade': 'technical support', 'configuration': 'technical support', 'maintenance': 'technical support', 'infrastructure': 'technical support', 'implementation': 'technical support', 'automation': 'technical support', 'deployment': 'technical support', 'setup': 'technical support', 'workflow': 'technical support', 'browser': 'technical support', 'driver': 'technical support', 'sap': 'technical support', 'quickbooks': 'technical support', 'xero': 'technical support', 'platform': 'technical support', 'system': 'technical support', 'connectivity': 'technical support', 'docker': 'technical support', 'saas': 'technical support', 'plugin': 'technical support', 'device': 'technical support', 'firewall': 'technical support', 'shopify': 'technical support', 'woocommerce': 'technical support', 'magento': 'technical support', 'jira': 'technical support', 'smartsheet': 'technical support', 'node.js': 'technical support', 'laravel': 'technical support', 'firebase': 'technical support', 'aws': 'technical support', 'google cloud platform': 'technical support', 'google cloud': 'technical support', 'ibm cloud': 'technical support', 'azure': 'technical support', 'mysql': 'technical support', 'postgresql': 'technical support', 'redis': 'technical support', 'cassandra': 'technical support', 'mongodb': 'technical support', 'oracle': 'technical support', 'git': 'technical support', 'github': 'technical support', 'ansible': 'technical support', 'terraform': 'technical support', 'kubernetes': 'technical support', 'ubuntu': 'technical support', 'windows': 'technical support', 'macos': 'technical support', 'android': 'technical support', 'macbook': 'technical support', 'laptop': 'technical support', 'notebook': 'technical support', 'workstation': 'technical support', 'equipment': 'technical support', 'projector': 'technical support', 'monitors': 'technical support', 'electronics': 'technical support', 'firmware': 'technical support', 'endpoint': 'technical support', 'microsoft teams': 'technical support', 'microsoftteams': 'technical support', 'webex': 'technical support', 'videoconference': 'technical support', 'outlook': 'technical support', 'email': 'technical support', 'microsoft onenote': 'technical support', 'visualstudiocode': 'technical support', 'ide': 'technical support', 'adobe creative cloud': 'technical support', 'adobe_creative_cloud': 'technical support', 'adobecreativecloud': 'technical support', 'adobe premiere pro': 'technical support', 'adobeaudition': 'technical support', 'inkscape': 'technical support', 'pytorch': 'technical support', 'tensorflow': 'technical support', 'python': 'technical support', 'express.js': 'technical support', 'alteryx': 'technical support', 'rapidminer': 'technical support', 'zapier': 'technical support', 'moodle': 'technical support', 'monday.com': 'technical support', 'clickup': 'technical support', 'zoho books': 'technical support', 'duolingo': 'technical support', 'gaming': 'technical support', 'smart türschloss': 'technical support', 'vr': 'technical support', 'vr-headsets': 'technical support', 'scalability': 'technical support', 'activation': 'technical support', 'cleanup': 'technical support', 'technical specifications': 'technical support', 'technical support': 'technical support', 'browserproblem': 'technical support', 'software compatibility': 'technical support', 'hubspot': 'technical support', 'upload': 'technical support',
        # security
        'security': 'security', 'cybersecurity': 'security', 'virus': 'security', 'malware': 'security', 'antivirus': 'security', 'infection': 'security', 'encryption': 'security', 'dataencryption': 'security', 'breach': 'security', 'privacy': 'security', 'dataprivacy': 'security', 'dataprotection': 'security', 'data protection': 'security', 'datasecurity': 'security', 'compliance': 'security', 'security compliance': 'security', 'hipaa': 'security', 'authentication': 'security', 'authorization': 'security', 'credential': 'security', 'credentials': 'security', 'permissions': 'security', 'permission': 'security', 'access': 'security', 'accesscontrol': 'security', 'access controls': 'security', 'unauthorizedaccess': 'security', 'integrity': 'security', 'confidentiality': 'security', 'vulnerability': 'security', 'risk': 'security', 'protection': 'security', 'safety': 'security', 'verification': 'security', 'userroles': 'security',
        # bugs & performance
        'performance': 'bugs & performance', 'bug': 'bugs & performance', 'crash': 'bugs & performance', 'crashes': 'bugs & performance', 'error': 'bugs & performance', 'glitch': 'bugs & performance', 'malfunction': 'bugs & performance', 'outage': 'bugs & performance', 'disruption': 'bugs & performance', 'network': 'bugs & performance', 'connection': 'bugs & performance', 'syncing': 'bugs & performance', 'synchronisationsfehler': 'bugs & performance', 'compatibility': 'bugs & performance', 'compatibilities': 'bugs & performance', 'compatibilityissues': 'bugs & performance', 'incompatibility': 'bugs & performance', 'incompatible': 'bugs & performance', 'optimization': 'bugs & performance', 'high traffic': 'bugs & performance', 'traffic': 'bugs & performance', 'overload': 'bugs & performance', 'alert': 'bugs & performance', 'issue': 'bugs & performance', 'mismatch': 'bugs & performance', 'discrepancy': 'bugs & performance', 'inconsistency': 'bugs & performance', 'inaccuracy': 'bugs & performance', 'bitrate': 'bugs & performance', 'unsuccessful': 'bugs & performance', 'consistency': 'bugs & performance', 'adjustment': 'bugs & performance', 'conflict': 'bugs & performance', 'protocols': 'bugs & performance',
        # product feedback
        'documentation': 'product feedback', 'documentations': 'product feedback', 'guidelines': 'product feedback', 'feedback': 'product feedback', 'guidance': 'product feedback', 'feature': 'product feedback', 'product': 'product feedback', 'training': 'product feedback', 'analytics': 'product feedback', 'data analytics': 'product feedback', 'data analysis': 'product feedback', 'datavisualization': 'product feedback', 'reporting': 'product feedback', 'problem reporting': 'product feedback', 'dashboard': 'product feedback', 'metrics': 'product feedback', 'analysis': 'product feedback', 'improvement': 'product feedback', 'enhancement': 'product feedback', 'customization': 'product feedback', 'userexperience': 'product feedback', 'user experience': 'product feedback', 'user interface': 'product feedback', 'model': 'product feedback', 'modelcalibration': 'product feedback', 'accuracy': 'product feedback', 'precision': 'product feedback', 'algorithm': 'product feedback', 'website': 'product feedback', 'tutorial': 'product feedback', 'suggestion': 'product feedback', 'limitation': 'product feedback', 'system requirements': 'product feedback', 'systemrequirements': 'product feedback', 'systemrequirement': 'product feedback', 'requirement': 'product feedback', 'specification': 'product feedback', 'option': 'product feedback', 'livestream': 'product feedback', 'live stream': 'product feedback', 'real-time': 'product feedback',
        # sales & marketing
        'sales': 'sales & marketing', 'marketing': 'sales & marketing', 'digital marketing': 'sales & marketing', 'inboundmarketing': 'sales & marketing', 'content marketing': 'sales & marketing', 'contentmarketing': 'sales & marketing', 'social media marketing': 'sales & marketing', 'socialmediamanagement': 'sales & marketing', 'seo': 'sales & marketing', 'strategy': 'sales & marketing', 'digital strategy': 'sales & marketing', 'digitalstrategy': 'sales & marketing', 'contentstrategy': 'sales & marketing', 'campaign': 'sales & marketing', 'campaigns': 'sales & marketing', 'digital campaigns': 'sales & marketing', 'digitalcampaign': 'sales & marketing', 'ad': 'sales & marketing', 'adcreative': 'sales & marketing', 'adtargeting': 'sales & marketing', 'adplacement': 'sales & marketing', 'adblocking': 'sales & marketing', 'branding': 'sales & marketing', 'brand': 'sales & marketing', 'brand growth': 'sales & marketing', 'brandgrowth': 'sales & marketing', 'growth': 'sales & marketing', 'expansion': 'sales & marketing', 'target': 'sales & marketing', 'audience': 'sales & marketing', 'audiencereach': 'sales & marketing', 'engagement': 'sales & marketing', 'conversion': 'sales & marketing', 'tracking': 'sales & marketing', 'online presence': 'sales & marketing', 'onlinepresence': 'sales & marketing', 'ecommerce': 'sales & marketing', 'e-commerce': 'sales & marketing', 'launch': 'sales & marketing', 'success': 'sales & marketing', 'market': 'sales & marketing', 'trends': 'sales & marketing', 'competition': 'sales & marketing', 'content creation': 'sales & marketing', 'contentcreation': 'sales & marketing', 'contentdevelopment': 'sales & marketing', 'promotion': 'sales & marketing', 'rabatte': 'sales & marketing', 'diienstleistung': 'sales & marketing',
        # billing & payments
        'payment': 'billing & payments', 'billing': 'billing & payments', 'billing cycle': 'billing & payments', 'pricing': 'billing & payments', 'price': 'billing & payments', 'subscription': 'billing & payments', 'duplicatesubscriptions': 'billing & payments', 'abonnement': 'billing & payments', 'invoice': 'billing & payments', 'invoicing': 'billing & payments', 'refund': 'billing & payments', 'discount': 'billing & payments', 'charge': 'billing & payments', 'fee': 'billing & payments', 'unforeseen charges': 'billing & payments', 'credit card': 'billing & payments', 'credit cards': 'billing & payments', 'onlinezahlung': 'billing & payments', 'online payment platform': 'billing & payments', 'payment gateways': 'billing & payments', 'reconciliation': 'billing & payments', 'accounting': 'billing & payments', 'finance': 'billing & payments', 'financial': 'billing & payments', 'financing': 'billing & payments', 'investment': 'billing & payments', 'portfolio': 'billing & payments', 'forecast': 'billing & payments', 'forecasting': 'billing & payments', 'prognosis': 'billing & payments', 'budget': 'billing & payments', 'licensing': 'billing & payments', 'license': 'billing & payments', 'cancellation': 'billing & payments', 'longtermagreement': 'billing & payments', 'longtermcommitment': 'billing & payments', 'quotation': 'billing & payments', 'annual plan': 'billing & payments',
        # customer account
        'customer': 'customer account', 'account': 'customer account', 'login': 'customer account', 'registration': 'customer account', 'onboarding': 'customer account', 'support': 'customer account', 'customer service': 'customer account', 'assistance': 'customer account', 'inquiry': 'customer account', 'follow-up': 'customer account', 'request': 'customer account', 'consultation': 'customer account', 'advice': 'customer account', 'service': 'customer account', 'services': 'customer account', 'order': 'customer account', 'shipment': 'customer account', 'return': 'customer account', 'returns': 'customer account', 'warranty': 'customer account', 'third party': 'customer account', 'thirdparty': 'customer account', 'signing process': 'customer account', 'sign': 'customer account', 'signatures': 'customer account', 'adobe sign': 'customer account', 'adobesign': 'customer account', 'convenience': 'customer account', 'interaction': 'customer account',
        # data management
        'data': 'data management', 'data management': 'data management', 'datamanagement': 'data management', 'dataloading': 'data management', 'datahandling': 'data management', 'data source': 'data management', 'data_source': 'data management', 'datastream': 'data management', 'datatransfer': 'data management', 'datasynchronization': 'data management', 'transfer': 'data management', 'backup': 'data management', 'recovery': 'data management', 'storage': 'data management', 'datastorage': 'data management', 'dataloss': 'data management', 'loss': 'data management', 'verlust': 'data management', 'log': 'data management', 'records': 'data management', 'indexing': 'data management', 'duplicate': 'data management', 'datum': 'data management', 'format': 'data management', 'feed': 'data management', 'datafeed': 'data management',
        # corporate & medical
        'medical': 'corporate', 'healthcare': 'corporate', 'health': 'corporate', 'hospital': 'corporate', 'patient': 'corporate', 'patientdata': 'corporate', 'medicaldata': 'corporate', 'hr': 'corporate', 'employee': 'corporate', 'recruitment': 'corporate', 'team': 'corporate', 'teamwork': 'corporate', 'collaboration': 'corporate', 'project': 'corporate', 'projectmanagement': 'corporate', 'workflow': 'corporate', 'task': 'corporate', 'task management': 'corporate', 'taskmanagement': 'corporate', 'task tracking': 'corporate', 'taskassignment': 'corporate', 'task assignment': 'corporate', 'taskdelegation': 'corporate', 'resource': 'corporate', 'resource management': 'corporate', 'business': 'corporate', 'agency': 'corporate', 'coordination': 'corporate', 'alignment': 'corporate', 'meeting': 'corporate', 'discussion': 'corporate', 'learning': 'corporate', 'productivity': 'corporate', 'practices': 'corporate', 'progress tracking': 'corporate', 'progressmonitoring': 'corporate', 'goal': 'corporate', 'goals': 'corporate', 'international': 'general', 'general': 'general', 'communication': 'general', 'notification': 'general', 'change': 'general', 'timeline': 'general', 'schedule': 'general', 'flexiblescheduling': 'general', 'flexibility': 'general', 'routine': 'general', 'nighttime': 'general', 'rejection': 'general', 'disclaimer': 'general', 'humanerror': 'general', 'concern': 'general', 'incident': 'incident', 'investigation': 'incident', 'resolution': 'incident', 'urgent': 'incident', 'urgency': 'incident', 'disappearance': 'incident'
    }
    df['tag_4'] = df['tag_4'].str.strip().map(map_tag_4).fillna(df['tag_4'])

    rescate_tag4 = {
        'technical support': [
            'outdated software', 'databaseproblem', 'serverconfiguration', 'sage', 'setting', 
            'version', 'datatransmission', 'tooling', 'softwareupgrade', 'audiosettings', 
            'telecom', 'outdateddrivers', 'excelintegration', 'formatting', 'wordpress'
        ],
        'bugs & performance': ['overheating', 'response', 'minimum requirement'],
        'product feedback': ['function', 'list', 'solution', 'experience', 'clarification', 'informationrequest', 'outdatedcontent', 'datadriven'],
        'sales & marketing': ['markettrend', 'returnoninvestment', 'google', 'digitalstrategyservices', 'awareness'],
        'security': ['control', 'informationsecurity', 'key']
    }
    map_rescate_v4 = {val: key for key, values in rescate_tag4.items() for val in values}
    df['tag_4'] = df['tag_4'].replace(map_rescate_v4)

    rescate_tag4_v2 = {
        'technical support': [
            'transmission', 'technology', 'cache', 'monitoring', 'hadoop', 'airtable', 'development', 'adobe', 'erp', 'nuendo', 'software update', 
            'patch', 'application', 'intellij', 'remotework', 'router', 'microsoft'
        ],
        'security': ['accesscontrols', 'password'],
        'bugs & performance': ['unexpected', 'protocol', 'delay'],
        'product feedback': ['management', 'correction', 'content', 'process'],
        'sales & marketing': ['digitalcampaigns', 'targeting', 'lead']
    }
    map_rescate_mid = {val: key for key, values in rescate_tag4_v2.items() for val in values}
    df['tag_4'] = df['tag_4'].replace(map_rescate_mid)


    #--------------------- E. tag 5
    df['tag_5'] = df['tag_5'].astype(str).str.split(',', n=1, expand=True)[0]
    df['tag_5'] = df['tag_5'].str.strip().replace('', 'not provided').fillna('not provided')

    group_tag5 = {
        'technical support': [
            'technical support', 'tech', 'technology', 'it', 'software', 'hardware', 'driver', 'browser', 'plugin','nas', 'qnap', 'matlab', 'code', 'firmware', 'erp', 'softwareupdate', 'software integration',
            'smoothintegration', 'smooth', 'backend', 'cloud', 'ibmcloud', 'microsoft', 'application', 'computing','machine', 'softwareasaservice', 'softwareanforderung', 'microsoftdynamics365', 'availability',
            'jenkins', 'monitoring', 'dependency', 'memory', 'recent', 'method', 'signal', 'future', 'rapidminer','tensorflow', 'functionality', 'ifttt', 'onenote', 'clickup', 'office', 'e-mail', 'mondaycom','knime', 'django', 'magento', 'outdated'
        ],
        'product feedback': [
            'product feedback', 'improvement', 'enhancement', 'recommendation', 'dataanalysis', 'datenauswertung','real-time analytics', 'query', 
            'interface', 'detail', 'procedure', 'webinar', 'google keep','website', 'design', 'accuracy', 'refinement', 'segmentation', 'model', 'content'
        ],
        'bugs & performance': ['bugs & performance', 'problem', 'overheating', 'syncissue', 'connectionproblem', 'specific error message'],
        'security': ['security', 'breach', 'data leak', 'prevention', 'assurance', 'accesscontrols', 'sensitivity', 'sensitivität'],
        'sales & marketing': ['sales & marketing', 'socialmedia', 'social media', 'mailchimp', 'salesforce', 'powerbi', 'sendgrid','adcontent', 'e-commerce', 'online', 'squarespace', 'contentcreation', 'wachstum', 'livestream', 'tracking'],
        'billing & payments': ['billing & payments', 'creditcard', 'accounting'],
        'customer account': ['customer account', 'pdffiller', 'promptness', 'prompt'],
        'data management': ['data management', 'datastorage', 'dataimport'],
        'corporate': ['corporate', 'discussion', 'hr', 'humanerror', 'remotework', 'writing', 'course', 'effort','role', 'projecttracking', 'teamsuggestion'],
        'general': ['general', 'setting', 'concern', 'strategy_update'],
        'not provided': ['not provided']
    }
    map_tag_5 = {val: key for key, values in group_tag5.items() for val in values}
    df['tag_5'] = df['tag_5'].map(map_tag_5).fillna(df['tag_5'])

    rescate_tag5 = {
        'technical support': ['adobe', 'creativecloud', 'adobecreativecloud', 'adobepremierepro', 'alternative', 'technicalsupport'],
        'data management': ['indexing'],
        'sales & marketing': ['socialmedia', 'digitalmarketing'],
        'product feedback': ['dataanalysis'],
        'customer account': ['customeraccount'],
        'billing & payments': ['billingpayments'],
        'bugs & performance': ['bugsperformance']
    }
    map_rescate_v5 = {val: key for key, values in rescate_tag5.items() for val in values}
    df['tag_5'] = df['tag_5'].replace(map_rescate_v5)

    rescate_tag5_v2 = {
        'technical support': [
            'scanner', 'software upgrade', 'caching', 'teams', 'alteryx', 'discord', 'router', 'avidprotools', 'microsoft_azure', 'software conflict', 'microsoft teams', 'desktop', 'viber', 'internetconnection', 
            'audio', 'nuendo', 'git', 'debian', 'webcamsettings', 'php', 'mcafee', 'ios', 'typo3', 'firewalls', 'system requirements', 'recent update', 'notion', 'software as a service', 'systemupdate', 
            'sage accounting', 'scan', 'file', 'monitor', 'ansible', 'drupal', 'smart-tracker', 'widgets', 'softwareintegration', 'frame rate', 'scanning', 'bitbucket', 'saperp', 'intellij idea', 'toolintegration', 'software updates', 'software conflicts', 'display', 'scikit-learn', 
            'hadoop', 'smarthomehub', 'visual studio code', 'bitrate', 'engine', 'eclipse', 'hardwarerequirements', 'sqlserver', 'ibm', 'gaming', 'softwareissue', 'software update', 'zoom', 'gcp', 'container', 'kaspersky'
        ],
        'security': ['security breach', 'information security', 'multifactorauthentication', 'fulldiskencryption', 'security software', 'dataprotection', 'safeguard', 'authorization', 'multi-factor', 'riskmanagement', 'unauthorized access', 'datumsicherheit', 'informationsecurity', 'potentialrisk', 'access controls'],
        'sales & marketing': [
            'digital channels', 'digitalmarketingservices', 'online presence', 'socialmediamarketing', 'launch', 'adblocking', 'email marketing', 'campaignanalysis',
            'campaign launch', 'commerce', 'emailmarketing', 'offer', 'adblocker', 'tactics', 'channel', 'businessdevelopment', 'conversions', 'emailcampaigns', 'ad',
            'market', 'adcreative', 'markenentwicklung', 'markenwachstum', 'digitalcampaign', 'adplacement', 'inboundmarketing', 'brand growth', 'successstories'
        ],
        'product feedback': [
            'review', 'data tracking', 'learning', 'easeofuse', 'usability', 'solution request', 'multilingual', 'guide', 'diagnosis', 'action', 'progress', 'customizable', 'customer experience', 'requirements', 
            'prognosis', 'proposal', 'accessibility', 'translation', 'solutions', 'detailed information', 'data feed', 'clarification', 'progress tracking', 'data integrity', 'datamodels', 'datafeed', 
            'revision', 'explanation', 'progresstracking', 'studentengagement', 'survey', 'data retrieval', 'consistency', 'guidelines', 'tutorial', 'demonstration', 'informationrequest'
        ],
        'billing & payments': [
            'budgeting', 'payment methods', 'budget', 'spending', 'payment plan', 'exchange', 'unpaid', 'roi', 'financialreporting', 'payment option', 'payment method', 'banküberweisung', 'expensetracking', 
            'monthly', 'amount', 'abonnement', 'forecasting', 'accrual', 'premium plans', 'longtermcommitment', 'longtermcommitments'
        ],
        'data management': ['dataflow', 'datainconsistency', 'import', 'loss', 'datamigration', 'datatracking', 'data analytics', 'data integration', 'dataingestion', 'dataintegration', 'export', 'informationstorage'],
        'corporate': [
            'task management', 'consultation', 'initiative', 'teams', 'patientinformation', 'teammeeting', 'new member', 'diagnosis', 'task', 'portfoliomanagement', 
            'customer relationship', 'business', 'taskmanagement', 'partnership', 'team', 'planning', 'teaminteraction', 'interview', 'company'
        ],
        'bugs & performance': ['critical', 'mismatch', 'high traffic', 'duplicate', 'load speed', 'delay', 'overload', 'glitch'],
        'customer account': ['cancellation', 'quick assistance', 'complaint']
    }
    map_rescate_tag5_v2 = {val: key for key, values in rescate_tag5_v2.items() for val in values}
    df['tag_5'] = df['tag_5'].replace(map_rescate_tag5_v2)

    rescate_tag5_v3 = {
        'technical support': [
            'coding', 'macos', 'updates', 'ressource', 'mysql', 'systemrequirements', 'expressjs', 'docker', 'mongodb', 'evernote', 'hubspot', 'softwareconflict', 
            'formatting', 'gitlab', 'wordpress', 'operating system', 'sap_erp', 'machinelearning', 'microsoft azure', 'nodejs', 'smarthome', 'products', 'projectmanagementsaas', 'protocol'
        ],
        'security': ['risk', 'permission', 'security protocols', 'password', 'credential', 'unauthorizedaccess', 'key', 'certification', 'expiration', 'cookie', 'patientdata', 'medicaldata', 'medical data'],
        'product feedback': ['algorithm', 'requirement', 'metrics', 'visualization', 'innovation', 'prediction'],
        'billing & payments': ['fees', 'fee', 'bank', 'credit cards', 'credit card', 'paypal', 'cost', 'forecast'],
        'sales & marketing': ['outreach', 'digitalcampaigns', 'digitalstrategy', 'socialmediamanagement', 'traffic'],
        'corporate': ['hospital', 'agency', 'flexiblescheduling', 'timeline', 'productivity', 'coordination'],
        'bugs & performance': ['malfunction', 'glitch'],
        'customer account': ['request', 'help', 'phone call'],
        'incident': ['urgency']
    }
    map_rescate_tag5_v3 = {val: key for key, values in rescate_tag5_v3.items() for val in values}
    df['tag_5'] = df['tag_5'].replace(map_rescate_tag5_v3)


    #--------------------- F. tag 6
    map_tag_6_final = {
        # technical support (it, apps, infra, herramientas)
        'tech support': 'technical support', 'it': 'technical support', 'technical support': 'technical support','maintenance': 'technical support', 'software': 'technical support', 'hardware': 'technical support',
        'technical': 'technical support', 'integration': 'technical support', 'update': 'technical support','upgrade': 'technical support', 'configuration': 'technical support', 'server': 'technical support',
        'api': 'technical support', 'plugin': 'technical support', 'tool': 'technical support','tools': 'technical support', 'troubleshooting': 'technical support', 'system': 'technical support',
        'saas': 'technical support', 'macos': 'technical support', 'docker': 'technical support','cache': 'technical support', 'airtable': 'technical support', 'postgresql': 'technical support',
        'firebase': 'technical support', 'crm': 'technical support', 'nas': 'technical support','redis': 'technical support', 'cassandra': 'technical support', 'connectivity': 'technical support',
        'driver': 'technical support', 'protocol': 'technical support', 'platform': 'technical support','android': 'technical support', 'discord': 'technical support', 'smartsheet': 'technical support',
        'laravel': 'technical support', 'sensor': 'technical support', 'application': 'technical support','systemupdate': 'technical support', 'requirements': 'technical support', 'setup': 'technical support',
        'firewall': 'technical support', 'industrystandard': 'technical support', 'bundle': 'technical support','bundles': 'technical support',
        # security
        'security': 'security', 'encryption': 'security', 'virus': 'security', 'hipaa': 'security','compliance': 'security', 'accesscontrol': 'security', 'access': 'security', 'integrity': 'security',
        'privacy': 'security', 'confidentiality': 'security', 'protection': 'security', 'verification': 'security','antivirus': 'security', 'accesscontrols': 'security',
        # bugs & performance
        'performance': 'bugs & performance', 'bugs & performance': 'bugs & performance', 'crash': 'bugs & performance','bug': 'bugs & performance', 'network': 'bugs & performance', 'outage': 'bugs & performance',
        'disruption': 'bugs & performance', 'issue': 'bugs & performance', 'compatibility': 'bugs & performance','optimization': 'bugs & performance', 'problem': 'bugs & performance', 
        'conflict': 'bugs & performance','error': 'bugs & performance', 'alert': 'bugs & performance', 'transmission': 'bugs & performance',
        'inaccuracy': 'bugs & performance','performanceanalysis': 'bugs & performance', 'syncing': 'bugs & performance','synchronization': 'bugs & performance',
        # product feedback
        'documentation': 'product feedback', 'feedback': 'product feedback', 'guidance': 'product feedback','training': 'product feedback', 'analytics': 'product feedback', 'product feedback': 'product feedback',
        'product': 'product feedback', 'analysis': 'product feedback', 'feature': 'product feedback','dataanalysis': 'product feedback', 'dataanalytics': 'product feedback', 'reporting': 'product feedback',
        'customization': 'product feedback', 'tutorial': 'product feedback', 'dashboard': 'product feedback','website': 'product feedback', 'real-time': 'product feedback',
        # sales & marketing
        'sales & marketing': 'sales & marketing', 'sales': 'sales & marketing', 'marketing': 'sales & marketing','strategy': 'sales & marketing', 'engagement': 'sales & marketing', 'digital': 'sales & marketing',
        'socialmedia': 'sales & marketing', 'promotion': 'sales & marketing', 'campaign': 'sales & marketing','seo': 'sales & marketing', 'growth': 'sales & marketing', 'lead': 'sales & marketing',
        'onlinemarketing': 'sales & marketing', 'digital strategy': 'sales & marketing', 'online': 'sales & marketing','contentmarketing': 'sales & marketing', 'emailmarketing': 'sales & marketing', 'tracking': 'sales & marketing',
        'campagnenmanagement': 'sales & marketing', 'drupal commerce': 'sales & marketing',
        # billing & payments
        'billing & payments': 'billing & payments', 'investment': 'billing & payments', 'discount': 'billing & payments','portfolio': 'billing & payments', 'payment': 'billing & payments', 'subscription': 'billing & payments',
        'invoice': 'billing & payments', 'charge': 'billing & payments', 'finance': 'billing & payments','cost': 'billing & payments', 'revenue': 'billing & payments', 'accrued': 'billing & payments',
        'paymentterms': 'billing & payments', 'return': 'billing & payments', 'investmentportfolio': 'billing & payments','reconciliation': 'billing & payments', 'portfolios': 'billing & payments', 
        'portfoliomanagement': 'billing & payments','bank transfer': 'billing & payments', 'pricingdetailsservice': 'billing & payments',
        # customer account
        'customer account': 'customer account', 'customer': 'customer account', 'account': 'customer account','assistance': 'customer account', 'login': 'customer account', 'service': 'customer account',
        'onboarding': 'customer account', 'follow-up': 'customer account', 'call': 'customer account','help': 'customer account', 'inquiry': 'customer account', 'solution': 'customer account',
        # data management
        'data management': 'data management', 'recovery': 'data management', 'backup': 'data management','log': 'data management', 'storage': 'data management', 'data': 'data management', 'datatracking': 'data management',
        # corporate & medical
        'corporate': 'corporate', 'medical': 'corporate', 'patient': 'corporate', 'hospital': 'corporate','employee': 'corporate', 'resource': 'corporate', 'project': 'corporate', 'team': 'corporate',
        'productivity': 'corporate', 'discussion': 'corporate', 'collaboration': 'corporate','projectmanagement': 'corporate', 'workflow': 'corporate',
        # general & incidents
        'incident': 'incident', 'investigation': 'incident', 'resolution': 'incident', 'urgent': 'incident','urgency': 'incident', 'detection': 'incident', 'communication': 'general', 'notification': 'general',
        'information': 'general', 'general': 'general', 'routine': 'general', 'timeline': 'general','informationrequest': 'general', 'clarification': 'general', 'outcome': 'general', 'plan': 'general',
        'option': 'general', 'next step': 'general', 'detail': 'general',
    }
    df['tag_6'] = df['tag_6'].map(map_tag_6_final).fillna(df['tag_6'])

    rescate_tag6 = {
        'customer account': ['support'],
        'technical support': ['database', 'automation', 'implementation'],
        'security': ['breach'],
        'product feedback': ['enhancement'],
        'general': ['multilingual', 'translation']
    }
    map_remanentes_v6 = {val: key for key, values in rescate_tag6.items() for val in values}
    df['tag_6'] = df['tag_6'].replace(map_remanentes_v6)

    rescate_tag6_v2 = {
        'technical support': [
            'query', 'workload', 'system resources', 'norton360', 'norton', 'signal', 'electronics', 'software applications', 'repository', 'obs studio', 'cisco', 'apache hadoop', 'expressjs', 
            'node.js', 'dokumentenscanner', 'mac', 'inkscape', 'repair', 'simulink', 'module', 'editing', 'endpoint', 'software as a service', 'serverlogs', 'express.js', 'loading', 'java', 
            'dataset', 'misconfiguration', 'datatraveler', 'smart türschloss', 'kubernetes', 'screen recorder', 'windows', 'proxies', 'webex', 'mobile', 'technology', 'ios', 'ibm cloud', 'gnu compiler', 
            'saperp', 'avast', 'serverrestart', 'drupal', 'plugins', 'code', 'softwareconflict', 'recent update', 'outdated cache', 'sap_erp', 'betriebssystem', 'datamigration', 'datasystem', 
            'kvm', 'quickbooks', 'sap erp', 'chef', 'add_ins', 'microsoftteams', 'spss', 'monitors', 'troubleshoot', 'software compatibility', 'dynamics', 'monday.com', 'oracle', 'softwareupdate', 
            'system configuration', 'workstation', 'google cloud platform', 'patch', 'matlab', 'onenote', 'android studio', 'xero', 'ccleaner', 'desktop', 'scan', 'webhook', 'adobecreativecloud', 
            'scrivener', 'hardware requirements', 'wix', 'loading', 'google cloud platform'
        ],
        'security': [
            'keymanagement', 'safeguard', 'safety', 'authentication', 'permission', 'two-factor authentication', 'vertraulichkeit', 'unauthorized access', 'vulnerabilities', 'lossprevention', 'datalocking', 
            'access-control', 'userpermissions', 'multifactorauthentication', 'passwordmanagement', 'dataencryption', 'credential', 'assurance', 'networkpolicies'
        ],
        'sales & marketing': [
            'onlineadvertising', 'mailchimp', 'marketingautomation', 'brand', 'usergeneratedcontent', 'web traffic', 'onlinevisibility', 'adfatigue', 'market trend', 'campaigns', 'brandexpansion', 
            'targeting', 'socialmediamanagement', 'emailcampaign', 'digitalpresence', 'e-marketing', 'productpromotion', 'digitalmarketingservices', 'adplacement', 'campaignperformance', 'onlinevisibility', 'presence', 'channel', 'productvisibility'
        ],
        'product feedback': [
            'datum analytics tool', 'user experience', 'visualization', 'accuracy', 'segmentation', 'learning', 'validation', 'decisionsupport', 'customizabledashboards', 'data analysis', 
            'insight', 'explanation', 'datadrivendecisions', 'datamodeling', 'specification', 'writingexperience', 'realtime', 'predictive modeling', 'revision', 'metric', 'design'
        ],
        'billing & payments': [
            'spend', 'investmentsoftware', 'forecasting', 'budgeting', 'fees', 'charges', 'expense', 'billing plan', 'invoicing', 'unusual charges', 'taxefficiency', 'pricing models', 'banktransfer', 'bank transfers', 'credit card'
        ],
        'corporate': [
            'teamwork', 'decision-making', 'teamkollaboration', 'schedule', 'longterm', 'term', 'project oversight', 'task management', 'healthcare', 'medicaldatum', 'patientendaten', 
            'student', 'interaction', 'strategyupdate', 'interview', 'company', 'curriculum', 'financialanalysis', 'projectmanagementsoftware', 'due dates', 'backlog', 'business'
        ],
        'bugs & performance': ['load', 'data overflow', 'systemflaw', 'filecorruption', 'inconsistency', 'inconsistencies', 'glitch', 'downtime', 'delay', 'konflikt', 'contentmismatch'],
        'customer account': ['contact', 'products', 'customersupport', 'customer support', 'request', 'confirmation', 'response', 'callback', 'replacement', 'client']
    }
    map_rescate_tag6_v2 = {val: key for key, values in rescate_tag6_v2.items() for val in values}
    df['tag_6'] = df['tag_6'].replace(map_rescate_tag6_v2)
    

    #--------------------- G. tag 7
    group_tag7 = {
        'product feedback': ['product & feedback', 'product feedback'],
        'technical support': ['technical support'],
        'bugs & performance': ['bugs & performance'],
        'customer account': ['customer account'],
        'security': ['security'],
        'incident': ['incident'],
        'corporate': ['corporate'],
        'data management': ['data management'],
        'sales & marketing': ['sales & marketing'],
        'general': ['other', 'general', 'communication'],
    }
    map_tag_7 = {val: key for key, values in group_tag7.items() for val in values}
    df['tag_7'] = df['tag_7'].str.strip().replace(map_tag_7)
    

    rescate_tag7 = {
        'technical support': [
            'databaseconnectivity', 'digital tools', 'aftereffects', 'moodle', 'passwordreset', 'settings', 'confluence', 'adobe', 'system requirements', 'nas', 'technology', 
            'telephone', 'upload', 'software integration', 'platform', 'pytorch', 'switch', 'sabrent', 'servicerestart', 'macbook', 'express.js', 'repair', 'matlab', 'load time', 
            'outdated libraries', 'microsoftazure', 'cable', 'software updates', 'ram', 'linuxmint', 'filesharing', 'deployment', 'sql server', 'firebase', 'chef', 
            'operatingsystem', 'raid-controller', 'magento', 'uptodatesoftware', 'laptop', 'router', 'browser', 'sage', 'automation', 'hub', 'systems', 'cloudfunctions', 
            'incompatible plugin', 'code', 'airtable', 'initialization', 'caching', 'microsoft', 'speed', 'office365', 'alteryx', 'raid', 'fix', 'datamigration', 
            'tag', 'scalability', 'synchronisieren', 'electronicsignature', 'navigation','rapidminer', 'firmware', 'sap', 'usb', 'aws', 'smart home sensor', 'editingcapabilites','visual studio code', 'errorlogging'
        ],
        'security': [
            'risk', 'permission', 'gdpr', 'credentials', 'medical data', 'cyberattack', 'accessmanagement', 'informationsafety', 'cyberthreats', 'unauthorized', 'multi-factor authentication', 
            'malware', 'riskassessment', 'riskmanagement', 'prevention', 'security software', 'credential', 'informationsecurity', 'sensitivity',
            'vulnerability', 'unsecured', 'securitymeasure', 'sensitivedata', 'password', 'dataprotection', 'safeguard', 'secretmanagement', 'unauthorizedaccess','patientdata'
        ],
        'product feedback': [
            'notwendigkeit', 'dat analysis', 'insight', 'algorithms', 'analysistools', 'predictivemodeling', 'insights', 'specification', 'bi', 'outdated', 'inaccuracy', 
            'solutions', 'correction', 'explanation', 'userexperience', 'visibility', 'dataaccuracy', 'datavisualization', 'parameter', 'design', 'inconsistency', 'requirements', 'monitor', 
            'webinar', 'market analysis', 'segmentation', 'data visualization', 'diagnostic', 'realtimeanalysis', 'real-time', 'accuracy', 'statistics', 'predictiveanalytics', 'document', 'best practices'
        ],
        'sales & marketing': [
            'advertisement', 'testimonials', 'social media management', 'onlinepresence', 'email marketing', 'engagementstrategy', 'realtimetracking', 'campaignmanagement', 'digitalcampaign', 
            'strategyservices', 'adcontent', 'advertising', 'usergeneratedcontent', 'online visibility', 'ppc', 'streaming', 'salesstrategy', 'digitalmarketing', 'ad spending', 'brandexpansion'
        ],
        'billing & payments': [
            'bank transfer', 'quote', 'forecast', 'payment processing', 'charge', 'financial','onlinepaymentsystem', 'banktransfer', 'fees', 'payment plan', 'fee', 'expensetracking', 
            'creditcard', 'creditcards', 'cashflowmanagement', 'currency', 'bank transfers', 'finance', 'investment_returns', 'late fees', 'billing'
        ],
        'corporate': [
            'meetings', 'calendar', 'teamkollaboration', 'curriculum', 'teamefficiency', 'appointment', 'diversification', 'portfolio optimization', 'resourcemanagement', 
            'license', 'decisionmaking', 'management', 'strategydiscussion', 'practice', 'education', 'intelligence', 'diagnosis', 'company', 'notion', 'legal', 
            'resource allocation', 'hiringprocess', 'teammanagement', 'partnership', 'longtermagreement', 'scheduling', 'portfoliooptimization', 'strategy support', 
            'actionteam', 'workshop', 'hospital', 'consultation', 'action', 'teamwork', 'ressource'
        ],
        'bugs & performance': ['miscalculation', 'glitch', 'abnormal', 'crashing', 'network congestion', 'crash report issue', 'crash log', 'interference', 'downtime', 'unexplained', 'urgency', 'crash notification'],
        'customer account': ['free trials', 'order', 'confirmation', 'contactinformation', 'trial', 'interaction', 'callarrange', 'query', 'satisfaction', 'contact information','promptness', 'immediate']
    }
    map_rescate_tag7 = {val: key for key, values in rescate_tag7.items() for val in values}
    df['tag_7'] = df['tag_7'].replace(map_rescate_tag7)

    rescate_tag7_v2 = {
        'technical support': ['accessory', 'internetconnection', 'scanner', 'dataprocessing', 'refresh', 'component', 'datastorage', 'artificialintelligence', 'kit', 'resources'],
        'product feedback': ['dataintegration', 'casestudy', 'recommendations', 'requirement', 'casestudies', 'potential', 'vital'],
        'corporate': ['effective', 'multilingual', 'investmentanalytics', 'expansion', 'audits', 'investment analytics', 'vorteils vergleich', 'change', 'setting', 'ressourcemanagement'],
        'customer account': ['usermanagement', 'customerinteraction', 'concern', 'customerengagement'],
        'sales & marketing': ['lead', 'offer', 'traffic'],
        'bugs & performance': ['delay'],
        'general': ['thirdparty']
    }
    map_rescate_tag7_v2 = {val: key for key, values in rescate_tag7_v2.items() for val in values}
    df['tag_7'] = df['tag_7'].replace(map_rescate_tag7_v2)


    rescate_tag7_v3 = {
        'technical support': [
            'redis', 'crm', 'linux', 'zapier', 'protocol', 'microsoftoffice', 'discord', 'cassandra', 'dns', 'dependency', 'alternative', 'device', 'ibm', 
            'ruby', 'googlecloudplatform', 'oracle', 'jenkins', 'saperp', 'wordpress', 'cache', 'connectivity', 'quickbooksonline', 'softwareupdate',
            'equipment', 'softwareconflict', 'python', 'smartsheet'
        ],
        'security': ['security breach', 'safety', 'certification', 'secure', 'controls'],
        'sales & marketing': ['socialmediamanagement', 'influencermarketing', 'e-commerce', 'online', 'promotion', 'conversion', 'branding'],
        'product feedback': ['personalization', 'dataanalytics', 'data analytics', 'metric', 'product', 'discrepancy', 'improvement', 'reporting'],
        'billing & payments': ['budgeting', 'invoices', 'discount'],
        'corporate': ['cooperation', 'business', 'project', 'portfoliodiversification', 'plan', 'audit', 'healthcare', 'service'],
        'customer account': ['inquiry', 'response'],
        'bugs & performance': ['malfunction']
    }
    map_rescate_tag7_v3 = {val: key for key, values in rescate_tag7_v3.items() for val in values}
    df['tag_7'] = df['tag_7'].replace(map_rescate_tag7_v3)


    #--------------------- H. tag 8
    group_tag8 = {
        'product feedback': ['product & feedback', 'product feedback'],
        'technical support': ['technical support', 'tools', 'software', 'hardware', 'it'],
        'general': ['other', 'general', 'communication'],
        'corporate': ['corporate', 'healthcare']
    }
    map_tag_8 = {val: key for key, values in group_tag8.items() for val in values}
    df['tag_8'] = df['tag_8'].replace(map_tag_8)

    rescate_tag8 = {
        'technical support': [
            'macbookpro', 'git', 'serverlog', 'cache', 'driver', 'cable', 'microsoftdynamics365', 'rails', 'onrails', 'plugin', 'systems', 'addins', 'browser', 'adobeaftereffects', 
            'transmission', 'oracledatabase19c', 'platform', 'application', 'h2o.ai', 'sdk', 'smartsheet', 'matlab', 'mongodb4.4', 'bitbucket', 'jenkins', 'zoho', 'eclipse ide', 
            'tableau', 'pdffiller', 'avast', 'devices', 'azure', 'jira', 'zapier', 'postgresql', 'linux', 'equipment', 'webhooks', 'operatingsystem', 'firewalls', 
            'workflowautomation', 'mongodb', 'pytorch', 'loganalysis', 'upgrade', 'logging', 'macbookair', 'systemintegration', 'softwareupdate', 'connector', 'bitlocker', 
            'hardwareconflict', 'patch', 'firmware', 'redis', 'vpn', 'updates', 'windows', 'software platform', 'erp', 'alteryx'
        ],
        'security': [
            'rolebasedaccesscontrol', 'access_control', 'access control', 'access controls', 'unverschlüsseltes', 'medicaldatastorage', 'passwordstorage', 'sensitivity', 'databreaches', 'security protocols', 
            'multifactorauthentication', 'patientdata', 'unauthorized', 'measures', 'twofactorauthentication', 'riskassessment', 'integrity', 'aes', 'riskanalysis', 'access_control', 'security protocols'
        ],
        'product feedback': [
            'businessintelligence', 'information request', 'outdated', 'vital', 'predictivemodeling', 'analyticsinconsistency', 'validation', 'specifications', 'personalization', 'ai', 'predictiveinsights', 
            'customization', 'correction', 'requirement', 'userexperience', 'accuracy', 'visualization', 'statisticalmodels', 'predictive analytics', 'details', 'estimate', 'performanceoptimization', 'report', 'detail'
        ],
        'sales & marketing': [
            'expansion', 'targetaudience', 'content creation', 'adtext', 'digitalstrategies', 'usergeneratedcontent', 'socialmediamarketing', 'marketingstrategy', 'googleanalytics', 'dashboard', 'website', 
            'onlineprofile', 'socialmediamanagement', 'realtimeupdates', 'realtimedata', 'segmentation', 'brandrecognition', 'tracking', 'digitalstrategy', 'websiteoptimization', 'promotion'
        ],
        'billing & payments': ['payment', 'portfolio', 'taxefficiency', 'subscription', 'bank', 'creditcard', 'revenue', 'budget', 'invoice', 'banktransfer', 'financialgoals', 'banktransfers', 'portfoliooptimization', 'quickbooks', 'fees', 'quote'],
        'corporate': [
            'decision_making', 'hr', 'decisionmaking', 'strategyupdate', 'taskdelegation', 'goodpractice', 'partnership', 'longtermagreements', 'project', 'cooperation', 'teammanagement', 'business', 
            'agency', 'team management', 'allocation', 'competitor', 'successstories', 'plan', 'projekttermin', 'decision', 'regulations', 'resources', 'project management', 'organization', 'corporate', 'consulting'
        ],
        'bugs & performance': ['overload', 'malfunction', 'underperformance', 'inconvenience', 'timing', 'systemload', 'dependency', 'dataloss', 'session', 'crashlog', 'glitch'],
        'customer account': ['customersupport', 'reference number', 'interactiondecline', 'inquiry', 'customerservice', 'outreach', 'response', 'user', 'demo', 'returns'],
        'incident': ['urgently', 'dringend']
    }
    map_rescate_v8 = {val: key for key, values in rescate_tag8.items() for val in values}
    df['tag_8'] = df['tag_8'].replace(map_rescate_v8)


    #----------------------     4.  Re asignacion de nombres  y otros     --------------------------
    tag_names = {
        'tag_1': 'problem_category',
        'tag_2': 'issue_classification',
        'tag_3': 'issue_subcategory',
        'tag_4': 'issue_detail',
        'tag_5': 'resolution_category',
        'tag_6': 'resolution_subcategory',
        'tag_7': 'resolution_detail',
        'tag_8': 'additional_tag'
    }
    df.rename(columns=tag_names, inplace=True)

    column_order = [
        'subject','body','answer','queue','sector','specific_sector','priority','priority_level','language',
        'ticket_type',
        'problem_category', 'issue_classification', 'issue_subcategory', 'issue_detail', 'resolution_category', 
        'resolution_subcategory', 'resolution_detail', 'additional_tag'
    ]
    df = df[column_order]
    

    # Identificador de origen
    df['dataset_source'] = 'multi_lang_4'

    return df