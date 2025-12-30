import pandas as pd

def clean_multi_lang_5(filepath):
    #ver colab
    #https://colab.research.google.com/drive/1lIlTul9gOMncnqcN8VK9fKOcRkLaNP0M#scrollTo=BUbnQNjCl_aO

    df = pd.read_csv(filepath)

    #----------------------- 1. Limpieza estandar  -----------------------
    df.drop('version', axis=1, inplace=True)#eliminada por no relevante

    for column in df.columns:
        if df[column].dtype == 'object':
            df[column] = (df[column]
                .str.lower()
                .str.strip()
                .str.replace(r'\s+', ' ', regex=True))


    values_to_fill = {
        'subject': 'no matter',
        'answer': 'unanswered'
    }
    df = df.fillna(value=values_to_fill)

    
    tag_columns = [col for col in df.columns if col.startswith('tag_')]
    df[tag_columns] = df[tag_columns].fillna('not provided')
      


    #----------------------- 2. Mapeo y tratamientos de inesperados -----------------------
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



    #----------------------- 3. Transformacion -----------------------
    priority_map = {
        'low': 2,
        'medium': 3,
        'high': 4
    }
    df['priority_level'] = df['priority'].map(priority_map)

    lang_map = {
        'en': 'english',
        'de': 'german'
    }
    df['language'] = df['language'].map(lang_map)


    #--------------- A. tag 1
    mask = df['tag_1'].str.contains(',', na=False)
    columnas_tags = [f'tag_{i}' for i in range(1, 9)] # ['tag_1', 'tag_2', ..., 'tag_8']

    def repartir_fila(fila):
        valores = [v.strip() for v in str(fila['tag_1']).split(',')]
        nueva_data = {col: 'not provided' for col in columnas_tags}
        for i, valor in enumerate(valores[:8]):
            nueva_data[f'tag_{i+1}'] = valor
        return pd.Series(nueva_data)
    df.loc[mask, columnas_tags] = df[mask].apply(repartir_fila, axis=1)

    
    group_tag1 = {
        'customer service': ['support', 'customer', 'service', 'inquiry', 'communication', 'urgency'],
        'technical issue': ['bug', 'performance', 'outage', 'disruption', 'maintenance', 'systemausfall', 'crash', 'recovery', 'virus', 'software', 'api', 'notification', 'alert', 'synchronization', 'configuration'],
        'product & docs': ['feature', 'product', 'documentation', 'feedback', 'accessibility', 'functionality', 'ui', 'user interface'],
        'infrastructure': ['network', 'it', 'server', 'database', 'kubernetes', 'firewall', 'backup', 'connectivity', 'hardware', 'printer', 'display', 'touchscreen', 'audio', 'video', 'bluetooth', 'smart home'],
        'security & compliance': ['security', 'compliance', 'medical', 'health', 'login', 'account', 'access', 'license', 'warranty'],
        'data management': ['data', 'analytics', 'data discrepancy', 'data inconsistency', 'data loss'],
        'organization & strategy': ['strategy', 'organization', 'project', 'agile', 'training', 'change', 'integration', 'outlook'],
        'business & sales': ['billing', 'sales', 'finance', 'marketing', 'investment', 'lead', 'salesforce', 'erp', 'business'],
        'logistics': ['shipping', 'delivery']
    }
    map_tag1 = {val: key for key, values in group_tag1.items() for val in values}
    df['tag_1'] = df['tag_1'].apply(lambda x: map_tag1.get(x.lower().strip(), x) if isinstance(x, str) and '/' not in x and x != 'not provided' else x)


    #--------------- B. tag 2
    group_tag2 = {
        'technical issue': ['performance', 'it', 'disruption', 'outage', 'bug', 'crash', 'virus', 'alert', 'incident', 'maintenance', 'malware', 'issue', 'scalability', 'software conflict', 'software update', 'error', 'software', 'software integration', 'software compatibility', 'software issue', 'system issue', 'technical issue', 'server overload', 'system update', 'system congestion', 'troubleshooting', 'compatibility', 'software incompatibility', 'update', 'configuration', 'technical difficulty', 'stability', 'reliability', 'software error', 'service disruption', 'tech', 'technical', 'softwareintegration', 'outdated software'],
        'infrastructure': ['network', 'hardware', 'connectivity', 'database', 'infrastructure', 'server', 'backup', 'recovery', 'system', 'cloud', 'vpn', 'data storage', 'electronics', 'cloud platform', 'hadoop', 'display', 'raid', 'cloud-native', 'windows', 'microservice', 'firmware', 'device', 'netzwerk', 'login', 'access', 'data access', 'aws', 'cloud native', 'system integration'],
        'product & docs': ['documentation', 'feedback', 'feature', 'product', 'integration', 'usability', 'api', 'synchronization', 'optimization', 'analysis', 'ui/ux', 'dashboard', 'product integration', 'user experience', 'user interface', 'documentatio', 'documentatoin', 'improvement', 'enhancement', 'accessibility', 'website'],
        'security & privacy': ['security', 'compliance', 'data breach', 'data protection', 'data privacy', 'privacy', 'vulnerability', 'data security', 'confidentiality', 'data management', 'breach', 'data encryption', 'data leak', 'data exposure', 'access control', 'dataprotection', 'cybersecurity', 'cyberattack', 'encryption', 'cyber attack', 'attack', 'information privacy', 'data', 'data integration', 'data integrity', 'data discrepancy', 'data issue', 'threat', 'dataleak', 'phishing', 'authentication', 'data loss', 'data sync'],
        'business & finance': ['sales', 'payment', 'marketing', 'strategy', 'refund', 'billing', 'campaign', 'digital strategy', 'analytics', 'digital marketing', 'data analytics', 'data analysis', 'lead', 'pricing', 'brand development', 'saas', 'investment', 'branding', 'invoice', 'subscription', 'brand', 'growth', 'crm', 'digital campaigns', 'social media', 'brand growth', 'promotion', 'brand expansion', 'finance', 'financial', 'budget', 'digital campaign', 'investment analysis', 'brand management', 'seo', 'cost', 'brand visibility', 'financial planning', 'price', 'advertising', 'campaigns', 'discrepancy'],
        'customer service': ['tech support', 'support', 'training', 'guidance', 'technical support', 'customer support', 'service', 'assistance', 'onboarding', 'account', 'employee', 'hr', 'customer engagement', 'engagement', 'inquiry', 'customer service']
    }
    map_tag2 = {val: key for key, values in group_tag2.items() for val in values}
    df['tag_2'] = df['tag_2'].apply(lambda x: map_tag2.get(x.lower().strip(), x) if isinstance(x, str) and '/' not in x and x != 'not provided' else x)


    #-------------------- C. tag 3
    mask_multivalue = df['tag_3'].str.contains(',', na=False)

    df.loc[mask_multivalue, 'tag_4'] = df.loc[mask_multivalue, 'tag_3'].str.split(',').str[1].str.strip()
    df.loc[mask_multivalue, 'tag_3'] = df.loc[mask_multivalue, 'tag_3'].str.split(',').str[0].str.strip()

    df['tag_3'] = df['tag_3'].str.lower()
    df['tag_4'] = df['tag_4'].str.lower()
    df['tag_3'] = df['tag_3'].astype(str).str.strip().str.lower()

    group_tag3 = {
        'technical issue': ['it', 'performance', 'disruption', 'outage', 'bug', 'software', 'virus', 'alert', 'maintenance', 'issue', 'software update', 'compatibility', 'scalability', 'malware', 'incident', 'crash', 'software conflict', 'troubleshooting', 'software compatibility', 'error', 'cyber threat', 'system update', 'server overload', 'system issue', 'software integration', 'outdated protocol', 'stability', 'outdated version', 'conflict', 'technical difficulty', 'protocol', 'it support', 'update', 'reliability', 'problem', 'technical issue', 'integration issue', 'system failure', 'outdated', 'platform update', 'software konflikt', 'software limitation', 'failure', 'integration problem', 'system error', 'integration error', 'slow response', 'technical malfunction', 'system conflict', 'software issue', 'fehler', 'system vulnerability', 'software vulnerability', 'algorithm change', 'software incompatibility', 'software version mismatch', 'installation', 'python', 'ruby on rails', 'sql', 'sdk', 'software development kit', 'patch', 'plugin', 'server restart', 'application issue', 'sync issue', 'browser compatibility', 'outdated systems', 'system details'],
        'infrastructure': ['network', 'hardware', 'recovery', 'system', 'server', 'backup', 'infrastructure', 'connectivity', 'firewall', 'database', 'platform', 'login', 'cloud', 'authentication', 'configuration', 'cache', 'firmware', 'access control', 'system integration', 'aws', 'docker', 'electronics', 'display', 'memory', 'sap', 'equipment', 'audio', 'erp', 'vpn', 'driver', 'gateway', 'cloud services', 'cisco ios', 'usb', 'microservices', 'google nest wifi', 'windows', 'printer', 'iot', 'touchscreen', 'azure', 'macos', 'swap', 'cloud-native', 'cloud platform', 'device', 'interface', 'cloud software', 'postgresql', 'redis', 'cassandra', 'microsoft sql server', 'system calibration', 'system configuration', 'hardware conflict'],
        'product & docs': ['documentation', 'feature', 'feedback', 'product', 'integration', 'optimization', 'api', 'synchronization', 'deployment', 'improvement', 'enhancement', 'ui', 'user experience', 'user interface', 'implementation', 'data visualization', 'visualization', 'dashboard', 'tutorial', 'content', 'workflow', 'digital tools', 'digital tool', 'document', 'instruction', 'query optimization', 'query', 'feeedback', 'portfolio', 'website', 'testing', 'verification', 'user guide', 'customization', 'capability', 'api integration', 'product line', 'guide', 'anleitung', 'product platform'],
        'security & privacy': ['security', 'data protection', 'compliance', 'vulnerability', 'confidentiality', 'data breach', 'data privacy', 'data', 'encryption', 'privacy', 'data analysis', 'data analytics', 'data integrity', 'data integration', 'breach', 'phishing', 'unauthorized access', 'data management', 'cybersecurity', 'cyberattack', 'cyber attack', 'data synchronization', 'data quality', 'data exposure', 'data inconsistency', 'healthcare', 'medical data', 'hospital', 'medical records', 'patient data', 'health data', 'medical', 'health', 'hospital system', 'healthcare it', 'risk', 'risk management', 'access', 'data processing', 'protection', 'threat', 'data leakage', 'authorization', 'data validation', 'digital signature', 'online security', 'dataprotection', 'data discrepancy', 'violation', 'network security', 'security breach', 'security update', 'hipaa', 'data accuracy', 'data monitoring', 'data access', 'medical systems', 'hospital networks', 'hospital systems', 'hospital infrastructure', 'medical information', 'medical sector', 'medical devices', 'medical documentation', 'patient information', 'records', 'healthit', 'access management', 'security management'],
        'business & finance': ['marketing', 'lead', 'refund', 'sales', 'payment', 'strategy', 'campaign', 'analytics', 'subscription', 'saas', 'digital marketing', 'digital strategy', 'investment', 'brand growth', 'billing', 'growth', 'discrepancy', 'seo', 'brand development', 'invoice', 'pricing', 'discount', 'brand expansion', 'digital campaign', 'financial', 'analysis', 'brand', 'brand awareness', 'brand visibility', 'branding', 'order', 'campaigns', 'investment optimization', 'promotion', 'revenue', 'transaction', 'investment strategy', 'return', 'finance', 'accounting', 'financial services', 'business growth', 'online visibility', 'productivity', 'campaign strategy', 'website traffic', 'cost analysis', 'advertising', 'digital expansion', 'business expansion', 'financial systems', 'financial loss', 'digital channels', 'market trend', 'targeting', 'investment analytics', 'investment analysis', 'digital campaigns', 'payment gateway', 'budget', 'audit', 'digital tactics', 'algorithm update', 'digital advertising', 'brand engagement', 'audience', 'audience targeting', 'product promotion', 'digital growth', 'industry trends', 'dispute', 'brand enhancement', 'report', 'campaign analysis', 'business strategy', 'content strategy', 'market analysis'],
        'customer service': ['tech support', 'support', 'training', 'account', 'service', 'technical support', 'customer support', 'guidance', 'employee', 'assistance', 'customer service', 'inquiry', 'hr', 'customer engagement', 'engagement', 'coordination', 'collaboration', 'communication', 'consultation', 'process improvement', 'task assignment', 'community', 'organizational structure', 'services', 'response', 'process automation', 'testing', 'research', 'information', 'shipment', 'delivery', 'process', 'ticket', 'request', 'warranty', 'alarm', 'consulting']
    }
    map_tag3 = {val: key for key, values in group_tag3.items() for val in values}
    df['tag_3'] = df['tag_3'].apply(lambda x: map_tag3.get(x.lower().strip(), x) if isinstance(x, str) and '/' not in x and x != 'not provided' else x)


    #-------------- D. tag 4
    group_tag4 = {
        'technical issue': [
            'tech support', 'it', 'performance', 'disruption', 'outage', 'bug', 'maintenance', 'software', 'alert', 'virus', 'troubleshooting', 'issue', 'system', 'compatibility', 
            'scalability', 'optimization', 'software update', 'update', 'technical support', 'firmware', 'automation', 'system update', 'software compatibility', 'cache', 
            'integrity', 'system integration', 'machine learning', 'malware', 'system restart', 'connectivity', 'crash', 'reliability', 'upgrade', 'setup', 'cyber threat', 'erp', 
            'system issue', 'password', 'outdated', 'software integration', 'stability', 'protocol', 'kubernetes', 'software issue', 'driver', 'server overload', 
            'system overload', 'technical problem', 'system log', 'memory leak', 'system scan', 'software conflict', 'protocols', 'integration issue'
        ],
        'infrastructure': ['recovery', 'hardware', 'network', 'server', 'platform', 'infrastructure', 'api', 'firewall', 'configuration', 'database', 'cloud', 'system restart', 'backup', 'architecture', 'oracle'],
        'product & docs': [
            'documentation', 'feedback', 'feature', 'product', 'integration', 'training', 'workflow', 'improvement', 'algorithm', 'content', 'monitoring', 'content creation', 
            'data integration', 'synchronization', 'user experience', 'digital tools', 'dashboard', 'deployment', 'tutorial', 'case study', 'innovation', 'process'
        ],
        'security & privacy': [
            'security', 'confidentiality', 'encryption', 'compliance', 'vulnerability', 'data protection', 'privacy', 'data breach', 'integrity', 'breach', 
            'authentication', 'unauthorized access', 'data privacy', 'dataprotection', 'threat', 'phishing', 'access management', 'protection', 'risk management', 
            'audit', 'cyber threat', 'information security', 'data security', 'data regulation', 'data leak', 'data access', 'outdated security', 'incident response'
        ],
        'business & finance': [
            'sales', 'marketing', 'strategy', 'refund', 'campaign', 'analytics', 'payment', 'saas', 'digital marketing', 'investment', 'discount', 'billing', 'lead', 
            'brand', 'subscription', 'seo', 'brand development', 'branding', 'financial', 'pricing', 'cost', 'analysis', 'agency', 'online visibility', 'business growth', 
            'campaigns', 'content marketing', 'invoice', 'promotion', 'brand expansion', 'advertising', 'payment gateway', 'return', 'campaign management', 'finance', 
            'transaction', 'financial services', 'market trend', 'target audience', 'digital campaign', 'content strategy'
        ],
        'customer service': [
            'support', 'account', 'investigation', 'training', 'customer support', 'assistance', 'incident', 'engagement', 'guidance', 'service', 'customer service', 
            'employee', 'online presence', 'customer engagement', 'login', 'inquiry', 'productivity', 'customer interaction', 'collaboration', 'business development', 
            'navigation', 'delay', 'flexibility', 'development', 'license', 'clarification', 'social media management', 'browser', 'customer experience', 'outreach', 'coordination'
        ],
        'healthcare': ['healthcare', 'medical data', 'hospital', 'data processing', 'patient data', 'hipaa', 'hospital system', 'patient information', 'healthcare data', 'medical information', 'health data']
    }
    map_tag4 = {val: key for key, values in group_tag4.items() for val in values}
    df['tag_4'] = df['tag_4'].apply(lambda x: map_tag4.get(x.lower().strip(), x) if isinstance(x, str) and '/' not in x and x != 'not provided' else x)

    group_tag4_v2 = {
        'technical issue': [
            'outdated software', 'data error', 'system version', 'system problem', 'patch', 'application', 'driver issue', 'platform issue', 'drupal', 'system delay', 'diagnostics',
            'integration problem', 'ubuntu', 'memory', 'data pipeline', 'system vulnerability', 'airtable', 'adobe', 'sdk', 'backend', 'software error', 'system failure', 'gcp', 'tool'
        ],
        'security & privacy': ['security protocol', 'security breach', 'safety', 'system security', 'cybersecurity', 'data encryption', 'resolution'],
        'healthcare': ['medical', 'hospital it', 'hospital infrastructure', 'medical records', 'hospital systems'],
        'business & finance': [
            'investment strategies', 'reconciliation', 'market growth', 'financial data', 'brand engagement', 'investment optimization', 'online advertising', 'targeting', 'payment methods', 'investment strategy', 'investment analysis'
        ],
        'product & docs': [
            'digital transformation', 'best practices', 'best practice', 'instruction', 'user guide', 'procedure', 'modeling', 'visual', 'insight', 'ui', 'user interface', 'document', 'enhancement', 'digital platform', 'accuracy', 'inaccuracy'
        ],
        'customer service': ['warranty', 'certification', 'consultation', 'advisory', 'order', 'decision-making', 'service inquiry', 'zapier'],
        'incident': ['urgent']
    }
    map_tag4_v2 = {val: key for key, values in group_tag4_v2.items() for val in values}
    df['tag_4'] = df['tag_4'].apply(lambda x: map_tag4_v2.get(x.lower().strip(), x) if isinstance(x, str) and '/' not in x and x != 'not provided' else x)

    rescate_tag4 = {
        'technical issue': [
            'third-party application', 'audio editing', 'microsoft dynamics', 'software product', 'report issue', 'redis', 'system challenge', 'data backup', 'router', 'system slowness', 'cache problem', 
            'system reboot', 'system diagnostics', 'package', 'system troubleshooting', 'rebooting', 'system driver', 'tools', 'data storage', 'software fehler', 'system synchronization', 
            'tool integration', 'platform optimization', 'system slowdown', 'cloud-native', 'datapipeline', 'iot', 'connection issue', 'softwareupdate', 'system cache', 'system conflict', 'system outage', 
            'software development', 'solution', 'outlook', 'real-time monitoring', 'system implementation', 'php', 'storage', 'scanner', 'system load', 'system integrity', 'speed', 'node.js', 'azure', 
            'platform update', 'system downtime', 'system patch', 'systemvulnerability', 'reinstallation', 'software patch', 'system error', 'indexing', 'microservice', 'cloud infrastructure', 
            'external device', 'microservices', 'data platform', 'cloud saas', 'app update', 'smart home', 'vpn', 'app issue', 'container services', 'reboot', 'app', 'technical requirement', 
            'softwareproblem', 'system application', 'dataprocessing', 'data availability', 'antivirus', 'sluggishness', 'digital tool', 'response time', 'synchronisation', 'ai', 'user management', 
            'version mismatch', 'technical difficulty', 'data entry', 'software development kit', 'internet', 'docker', 'system optimization', 'softwareintegration', 'microsoft'
        ],
        'security & privacy': [
            'threat detection', 'security improvement', 'security measures', 'attack', 'hospital security', 'threat prevention', 'sensitive data', 'control', 'network security', 'cyberattack', 'regulatory', 
            'permission', 'platform security', 'threat analysis', 'patient confidentiality', 'security standards', 'safeguarding', 'regulatory compliance', 'unauthorizedaccess', 'cyber threats', 'cyber'
        ],
        'business & finance': [
            'portfolio', 'digital tactics', 'forecast', 'customer targeting', 'investment data analytics', 'business analysis', 'portfolio return', 'investment analysis tool', 'tool investment', 'budget', 
            'financial firm', 'accounting', 'financial loss', 'business performance', 'brand messaging', 'analytics platform', 'brand recognition', 'reputation', 'campaign analysis', 'real-time insights', 
            'shopping behavior', 'setup fee', 'campaign delay', 'financial sector', 'digital campaigns', 'industry trend', 'brandexpansion', 'data-driven decision', 'digitalmarketing', 'data discrepancy', 
            'market analysis', 'advertisement', 'brand presence', 'brand strategy', 'market trends', 'cost analysis', 'fees', 'business expansion', 'financial institution', 'offer', 'e-commerce'
        ],
        'product & docs': [
            'instructions', 'system enhancement', 'analysis platform', 'audience engagement', 'accessibility', 'solutions', 'model', 'content management', 'platform optimization', 'documentations', 'evaluation', 
            'user interaction', 'service improvement', 'visualization', 'data analysis tool', 'usability', 'content development', 'prediction error', 'segmentation', 'decision support', 'testing', 
            'success story', 'standard', 'service quality', 'technical requirement', 'editing', 'design', 'reporting', 'product line', 'product integration'
        ],
        'healthcare': ['health it', 'hospital records', 'health', 'hospital data', 'telemedicine', 'hospital network', 'health sector', 'medical product', 'medical systems', 'hospital orders'],
        'customer service': [
            'plans', 'availability', 'challenge', 'content issue', 'nutzung', 'advice', 'hr', 'management', 'promotional offer', 'exchange', 'communication', 'customer', 'plan', 'audio', 'sprint', 'workshop', 
            'tech sector', 'free trial', 'dslr', 'alarm', 'data handling', 'user conflict', 'partnership', 'service package', 'consulting', 'resources', 'resource management', 'system guidance', 'priority', 'injury'
        ],
        'incident': ['emergency', 'urgency', 'unforeseen']
    }
    map_rescate_tag4 = {val: key for key, values in rescate_tag4.items() for val in values}
    df['tag_4'] = df['tag_4'].replace(map_rescate_tag4)

    rescate_tag4_v2 = {
        'technical issue': ['it support', 'outdated version', 'data loss', 'data issue', 'system usage', 'gateway', 'load', 'email', 'crm'],
        'customer service': ['messaging', 'ticket', 'custom plan', 'issue analysis'],
        'business & finance': ['portfolio management', 'packages', 'influencer', 'digital space', 'media management', 'traffic'],
        'corporate': ['organizational change'],
        'security & privacy': ['credential', 'data accessibility', 'customer data']
    }
    map_rescate_tag4_v2 = {val: key for key, values in rescate_tag4_v2.items() for val in values}
    df['tag_4'] = df['tag_4'].replace(map_rescate_tag4_v2)


    #-------------- tag 5
    map_tag_5 = {
        # TECHNICAL ISSUE (Bugs, Performance, Crashes, Desarrollo)
        'it': 'technical issue', 'performance': 'technical issue', 'disruption': 'technical issue','outage': 'technical issue', 'bug': 'technical issue', 'software': 'technical issue',
        'troubleshooting': 'technical issue', 'alert': 'technical issue', 'software update': 'technical issue',
        'system issue': 'technical issue', 'system update': 'technical issue', 'software conflict': 'technical issue',
        'vulnerability': 'technical issue', 'malware': 'technical issue', 'software compatibility': 'technical issue',
        'algorithm': 'technical issue', 'system restart': 'technical issue', 'system reboot': 'technical issue',
        'error': 'technical issue', 'system error': 'technical issue', 'critical issue': 'technical issue',
        'critical': 'technical issue', 'debugging': 'technical issue', 'software fehler': 'technical issue',
        'system failure': 'technical issue', 'system problem': 'technical issue', 'software problem': 'technical issue',
        'glitch': 'technical issue', 'malfunction': 'technical issue', 'system slowdown': 'technical issue',
        'crash': 'technical issue', 'technical problem': 'technical issue', 'technical difficulty': 'technical issue',
        'instability': 'technical issue', 'reinstallation': 'technical issue', 'rebooting': 'technical issue',
        'reboot': 'technical issue', 'softwareconflict': 'technical issue', 'system überlastung': 'technical issue',
        'application issue': 'technical issue', 'fix': 'technical issue', 'python': 'technical issue',
        'patch': 'technical issue', 'software incompatibility': 'technical issue', 'update issue': 'technical issue',
        # INFRAESTRUCTURA (Redes, Hardware, Cloud, OS)
        'recovery': 'infrastructure', 'hardware': 'infrastructure', 'server': 'infrastructure',
        'network': 'infrastructure', 'system': 'infrastructure', 'configuration': 'infrastructure',
        'platform': 'infrastructure', 'infrastructure': 'infrastructure', 'cache': 'infrastructure',
        'firewall': 'infrastructure', 'database': 'infrastructure', 'backup': 'infrastructure',
        'connectivity': 'infrastructure', 'firmware': 'infrastructure', 'api': 'infrastructure',
        'cloud': 'infrastructure', 'system security': 'infrastructure', 'device': 'infrastructure',
        'setup': 'infrastructure', 'system integration': 'infrastructure', 'authentication': 'infrastructure',
        'login': 'infrastructure', 'system cache': 'infrastructure', 'protocols': 'infrastructure',
        'protocol': 'infrastructure', 'log': 'infrastructure', 'environment': 'infrastructure',
        'postgresql': 'infrastructure', 'sap': 'infrastructure', 'kubernetes': 'infrastructure',
        'vpn': 'infrastructure', 'driver': 'infrastructure', 'caching': 'infrastructure',
        'microservice': 'infrastructure', 'microservices': 'infrastructure', 'container': 'infrastructure',
        'os': 'infrastructure', 'router': 'infrastructure', 'cloud infrastructure': 'infrastructure',
        'browser': 'infrastructure', 'cassandra': 'infrastructure', 'ubuntu': 'infrastructure',
        'storage': 'infrastructure', 'data storage': 'infrastructure', 'printer': 'infrastructure',
        'smart thermostat': 'infrastructure', 'netzwerk': 'infrastructure', 'infrastruktur': 'infrastructure',
        # PRODUCT & DOCS (UI/UX, Documentación, Feedback, Automatización)
        'documentation': 'product & docs', 'feedback': 'product & docs', 'feature': 'product & docs',
        'integration': 'product & docs', 'product': 'product & docs', 'optimization': 'product & docs',
        'content': 'product & docs', 'workflow': 'product & docs', 'deployment': 'product & docs',
        'automation': 'product & docs', 'tutorial': 'product & docs', 'implementation': 'product & docs',
        'content development': 'product & docs', 'website': 'product & docs', 'tools': 'product & docs',
        'tool': 'product & docs', 'digital tools': 'product & docs', 'visualization': 'product & docs',
        'navigation': 'product & docs', 'user experience': 'product & docs', 'improvement': 'product & docs',
        'machine learning': 'product & docs', 'application': 'product & docs', 'innovation': 'product & docs',
        'dashboard': 'product & docs', 'enhancement': 'product & docs', 'design': 'product & docs',
        'user guide': 'product & docs', 'customization': 'product & docs', 'usability': 'product & docs',
        'ui': 'product & docs', 'software development kit': 'product & docs', 'plugin': 'product & docs',
        'datarobot': 'product & docs', 'h2o.ai': 'product & docs', 'specification': 'product & docs',
        # SECURITY & PRIVACY (Seguridad, Datos, HIPAA, Healthcare)
        'security': 'security & privacy', 'healthcare': 'security & privacy', 'confidentiality': 'security & privacy',
        'compliance': 'security & privacy', 'access control': 'security & privacy', 'encryption': 'security & privacy',
        'privacy': 'security & privacy', 'data protection': 'security & privacy', 'data analysis': 'security & privacy',
        'medical data': 'security & privacy', 'patient data': 'security & privacy', 'patient privacy': 'security & privacy',
        'breach': 'security & privacy', 'monitoring': 'security & privacy', 'access management': 'security & privacy',
        'unauthorized access': 'security & privacy', 'sensitive data': 'security & privacy', 'risk': 'security & privacy',
        'audit': 'security & privacy', 'cybersecurity': 'security & privacy', 'hospital system': 'security & privacy',
        'medical information': 'security & privacy', 'medical records': 'security & privacy', 'hipaa': 'security & privacy',
        'telemedicine': 'security & privacy', 'data security': 'security & privacy', 'password reset': 'security & privacy',
        'hospital systems': 'security & privacy', 'emr': 'security & privacy', 'patient information': 'security & privacy',
        'hospital infrastructure': 'security & privacy', 'data privacy': 'security & privacy', 'data breach': 'security & privacy',
        'phishing': 'security & privacy', 'security measures': 'security & privacy', 'security policy': 'security & privacy',
        'data accuracy': 'security & privacy', 'dataprotection': 'security & privacy', 'hospitalsystem': 'security & privacy',
        'data management': 'security & privacy', 'data integrity': 'security & privacy', 'datapipeline': 'security & privacy',
        # BUSINESS & FINANCE (Ventas, Marketing, Finanzas, Estrategia)
        'sales': 'business & finance', 'strategy': 'business & finance', 'campaign': 'business & finance',
        'marketing': 'business & finance', 'analytics': 'business & finance', 'digital marketing': 'business & finance',
        'digital': 'business & finance', 'saas': 'business & finance', 'discount': 'business & finance',
        'refund': 'business & finance', 'social media': 'business & finance', 'pricing': 'business & finance',
        'investment': 'business & finance', 'brand': 'business & finance', 'growth': 'business & finance',
        'business growth': 'business & finance', 'business development': 'business & finance', 'advertising': 'business & finance',
        'financial': 'business & finance', 'billing': 'business & finance', 'budget': 'business & finance',
        'subscription': 'business & finance', 'transaction': 'business & finance', 'cost': 'business & finance',
        'market trend': 'business & finance', 'market analysis': 'business & finance', 'agency': 'business & finance',
        'revenue': 'business & finance', 'investment strategy': 'business & finance', 'lead': 'business & finance',
        'financial services': 'business & finance', 'online marketing': 'business & finance', 'branding': 'business & finance',
        'digital strategy': 'business & finance', 'market trends': 'business & finance', 'business operations': 'business & finance',
        'contract': 'business & finance', 'financial reporting': 'business & finance', 'digital growth': 'business & finance',
        # CUSTOMER SERVICE (Soporte, Procesos, RRHH, Operaciones)
        'tech support': 'customer service', 'support': 'customer service', 'technical support': 'customer service',
        'customer support': 'customer service', 'customer service': 'customer service', 'assistance': 'customer service',
        'training': 'customer service', 'guidance': 'customer service', 'inquiry': 'customer service',
        'service': 'customer service', 'account': 'customer service', 'employee': 'customer service',
        'project management': 'customer service', 'engagement': 'customer service', 'best practice': 'customer service',
        'efficiency': 'customer service', 'process': 'customer service', 'investigation': 'customer service',
        'urgent': 'customer service', 'onboarding': 'customer service', 'request': 'customer service',
        'coordination': 'customer service', 'collaboration': 'customer service', 'hr': 'customer service',
        'ticket management': 'customer service', 'user support': 'customer service', 'technical assistance': 'customer service',
        'client engagement': 'customer service', 'operation': 'customer service', 'organization': 'customer service'
    }
    df['tag_5'] = df['tag_5'].apply(lambda x:
        x if (not isinstance(x, str) or '/' in x or x == 'not provided')
        else map_tag_5.get(x.lower().strip(), x)
    )

    rescate_tag5 = {
        'technical support': [
            'speed', 'data issue', 'downtime', 'toolset', 'integration issue', 'dependencies', 'validation error', 'root cause', 'technical tools', 'payment gateway', 'data pipeline', 
            'caching issue', 'tech', 'system analysis', 'system connector', 'synchronisation', 'software error', 'systemaccess', 'test version', 'connection problem', 'system driver', 
            'integration challenges', 'platform integration', 'log issue', 'log rollback', 'service cache', 'sync', 'third party tool', 'systemrestart', 'ar', 'webhook', 
            'connection', 'cloud service', 'software enhancement', 'scaling', 'slowdown', 'data traffic', 'system tracking', 'system optimization', 'outdated protocol', 
            'code deployment', 'resolution', 'data refresh', 'system diagnostics', 'oracle', 'switch', 'windows 10', 'digital platform', 'restart issue', 'system bottleneck', 
            'cache clearing', 'website optimization', 'webpage', 'google keep', 'office equipment'
        ],
        'security & privacy': [
            'security solution', 'access', 'access log', 'safety measures', 'security improvement', 'safety patch', 'mitigation', 'credential security', 'exposure', 'prevention', 
            'threat evaluation', 'system integrity', 'security protocols', 'security alert', 'software vulnerability', 'compromise', 'accessmanagement', 'credentials', 
            'cyberattack', 'device security', 'sensitive resource', 'security patch', 'outdated security', 'cyber threat', 'sensitive', 'dataprotection'
        ],
        'business & finance': [
            'investment strategies', 'cost details', 'financial risk', 'financial decision', 'cost breakdown', 'financial institution', 'financial assistance', 'financial analysis', 
            'advertising budget', 'market awareness', 'market development', 'digital tactics', 'market', 'business operation', 'business strategy', 'financial industry', 
            'abrechnungssystem', 'budget reallocation', 'cost analysis', 'investment analysis', 'financial industry', 'return'
        ],
        'sales & marketing': [
            'content growth', 'customer loyalty', 'online growth', 'rebranding', 'audience engagement', 'marketing strategy', 'brand engagement', 'campaign management', 'audience targeting', 
            'audience reach', 'reach', 'digital footprint', 'outreach', 'marketing team', 'digital campaigns', 'social media marketing', 'website traffic', 'productpromotion', 
            'digital presence', 'digitalmarketing', 'market', 'digital channel', 'audience mismatch', 'campaign effectiveness', 'content management', 'segmentation', 'brand engagement'
        ],
        'product & docs': [
            'strategy improvement', 'process improvement', 'procedure', 'user friendly', 'quality', 'insights', 'analytics platform', 'user manual', 'data validation', 'validation', 
            'datenanalyse', 'success story', 'product improvement', 'methodology', 'inaccuracy', 'decision support', 'evaluation', 'knowledge base', 'report', 'reliability', 
            'metrics', 'behavior', 'operational efficiency', 'audience insights', 'creativity', 'misclassification', 'content issue', 'process optimization'
        ],
        'customer service': [
            'team', 'research', 'service capacity', 'advice', 'customer interaction', 'service optimization', 'customer experience', 'warranty', 'exchange', 
            'partnership', 'benefit', 'client support', 'communication', 'service offerings', 'service package', 'inquiry', 'replacement', 'messaging', 'e-signature'
        ],
        'healthcare': ['medical devices', 'medicaldata', 'patient', 'medical system', 'hospital environment', 'medical support', 'hospitalinfrastructure', 'healthcare data'],
        'incident': ['initiative', 'high priority', 'major', 'service disruption', 'impact', 'warning', 'traffic surge', 'unintentional', 'misconfiguration'],
        'corporate': ['leadership', 'projectmanagement', 'resources', 'planning', 'plans', 'internal review', 'decision-making', 'plan', 'policy']
    }
    map_rescate_tag5 = {val: key for key, values in rescate_tag5.items() for val in values}
    df['tag_5'] = df['tag_5'].replace(map_rescate_tag5)

    rescate_tag5_v2 = {
        'technical support': ['data exchange', 'data source', 'data retrieval', 'data reconciliation', 'outdated', 'outdated content', 'adjustment'],
        'billing & payments': ['payment method', 'packages'],
        'sales & marketing': ['digital services', 'media', 'creative', 'expansion', 'competitiveness', 'industry trend'],
        'product & docs': ['inspection', 'inconsistency'],
        'customer service': ['difficulty', 'concern']
    }
    map_rescate_tag5_v2 = {val: key for key, values in rescate_tag5_v2.items() for val in values}
    df['tag_5'] = df['tag_5'].replace(map_rescate_tag5_v2)

    rescate_tag5_v3 = {
        'technical issue': [
            'diagnostics', 'data availability', 'software compatibility', 'antivirus', 'installation', 'system overload', 'root cause', 'software bug', 'system stability', 
            'backend', 'dependency', 'synchronisation', 'android', 'microsoft', 'system requirements', 'system slowdown', 'reprocessing', 'password', 'system vulnerability', 'log', 'microservice', 'machine learning', 'functionality'
        ],
        'infrastructure': ['sap erp', 'gateway', 'device', 'payment gateway', 'cloud'],
        'security & privacy': ['patient confidentiality', 'data security', 'phishing', 'security policy', 'security update', 'hipaa', 'risk assessment', 'network security', 'password reset'],
        'business & finance': ['market growth', 'fee', 'brand visibility', 'digital campaign', 'payment methods', 'portfolio', 'invoice', 'investment optimization', 'financial loss', 'revenue', 'conversion rate', 'market trend', 'segmentation', 'brand expansion', 'discounts'],
        'product & docs': ['website', 'inaccuracy', 'target audience', 'audience targeting', 'recommendations', 'report', 'review', 'visualization', 'prediction', 'data validation'],
        'customer service': ['advice', 'setup', 'customer satisfaction', 'adjustment', 'capacity', 'challenge', 'management', 'support request', 'unresolved', 'influencer', 'consultation', 'customer experience'],
        'corporate': ['regulations', 'business process', 'organization', 'audit'],
        'incident': ['critical issue', 'critical', 'incident response', 'user impact']
    }
    map_rescate_tag5_v3 = {val: key for key, values in rescate_tag5_v3.items() for val in values}
    df['tag_5'] = df['tag_5'].replace(map_rescate_tag5_v3)




    #------------------- tag 6
    group_tag6_1 = {
        'technical issue': [
            'tech support', 'it', 'performance', 'software', 'maintenance', 'disruption', 'system', 'issue', 'troubleshooting', 'update', 'outage', 'alert', 'error', 
            'optimization', 'scalability', 'software update', 'technical support', 'compatibility', 'bug', 'cache', 'integrity', 'protocol', 'deployment', 
            'conflict', 'system integration', 'technology', 'system security', 'system update', 'system error', 'system integrity', 'connectivity', 
            'system issue', 'monitoring', 'synchronization', 'reinstallation', 'load', 'system scan', 'connection', 'automation', 'load balancing', 
            'system cache', 'diagnosis', 'cache issue', 'software issue', 'system restart', 'reboot', 'software conflict'
        ],
        'infrastructure': ['recovery', 'hardware', 'network', 'server', 'database', 'infrastructure', 'api', 'firewall', 'platform', 'backup', 'availability', 'resources', 'erp'],
        'product & docs': [
            'documentation', 'feedback', 'feature', 'product', 'guidance', 'best practices', 'improvement', 'algorithm', 'implementation', 'workflow', 'user experience', 
            'analysis', 'enhancement', 'tutorial', 'content', 'content creation', 'content marketing', 'process', 'customization', 'case study', 'best practice', 'correction'
        ],
        'security & privacy': [
            'confidentiality', 'security', 'compliance', 'investigation', 'encryption', 'privacy', 'sensitive data', 'information security', 'vulnerability', 
            'access control', 'data protection', 'unauthorized access', 'data breach', 'breach', 'risk', 'safety', 'risk management', 'regulation', 'protection', 'access management', 'policy'
        ],
        'business & finance': [
            'digital marketing', 'campaign', 'sales', 'discount', 'analytics', 'growth', 'pricing', 'branding', 'investment', 'advertising', 'cost', 'effectiveness', 
            'digital strategy', 'brand growth', 'marketing', 'lead', 'branding', 'promotion', 'visibility', 'financial', 'brand development', 'innovation', 
            'business development', 'business growth', 'seo', 'online visibility', 'data analytics', 'refund', 'payment', 'budget', 'billing', 'market', 'transaction', 'business'
        ],
        'customer service': [
            'support', 'customer support', 'assistance', 'engagement', 'service', 'customer service', 'customer engagement', 'agency', 'training', 'account', 
            'customer engagement', 'inquiry', 'response', 'employee', 'team', 'messaging', 'consultation', 'services'
        ],
        'healthcare': ['healthcare', 'patient data', 'medical data', 'telemedicine', 'hospital system'],
        'incident': ['urgent', 'incident', 'alert']
    }
    map_tag6_1 = {val: key for key, values in group_tag6_1.items() for val in values}
    df['tag_6'] = df['tag_6'].replace(map_tag6_1)

    group_tag6_2 = {
        'technical issue': [
            'dataprocessing', 'data issue', 'systemsecurity', 'memory leak', 'service connection', 'transaction log', 'speed', 'technology support', 'fehler', 'plugin', 'service disruption', 
            'system compatibility', 'tool', 'system software update', 'code', 'ubuntu', 'system check', 'networking', 'system reboot', 'log analysis', 'memory', 'system instability', 
            'gaming pcs', 'fix', 'app issue', 'keras', 'pytorch', 'neustart', 'restart', 'connection problem', 'software tools', 'rapidminer', 'gcp', 'sap'
        ],
        'security & privacy': [
            'soc', 'threat prevention', 'cyber threat', 'exploitation', 'security practice', 'sensitivity', 'sensitivedata', 'endpoint security', 
            'healthcare security', 'security solutions', 'credential', 'system breach', 'security best practices', 'sensitive', 'safeguard'
        ],
        'product & docs': [
            'document', 'creative content', 'content review', 'predictive', 'guide', 'analysis capability', 'digital transformation', 'product integration', 'digital tools', 
            'technique', 'analysis method', 'tips', 'standard', 'validation', 'outcome', 'technical specification', 'limitation'
        ],
        'sales & marketing': [
            'audience segment', 'trends', 'target', 'online advertising', 'brand messaging', 'campaign management', 'product promotion', 
            'promotional', 'market trends', 'expansion', 'trend', 'online interaction', 'search engine', 'influencers', 'digital presence'
        ],
        'business & finance': [
            'invoice generation', 'business information', 'loyalty', 'cost breakdown', 'online payment', 'investment strategy', 'cost estimation', 'financial organization', 'financial support'
        ],
        'corporate': [
            'project planning', 'decision-making', 'research', 'auditing', 'planning', 'potential consequence', 'reputational', 'long-term', 'information management', 
            'problem-solving', 'reassessment', 'industry standard', 'plan', 'business planning', 'business expansion', 'expertise'
        ],
        'healthcare': [
            'telehealth', 'patientdata', 'patient safety', 'medical documentation', 'hospital it', 'healthcare it', 'medical systems', 'patient care'
        ],
        'customer service': [
            're-training', 'agentur', 'prompt response', 'approach', 'issue resolution','customer', 'query', 'service options', 'customer interaction', 'user', 'vendor support'
        ],
        'incident': ['emergency']
    }
    map_tag6_2 = {val: key for key, values in group_tag6_2.items() for val in values}
    df['tag_6'] = df['tag_6'].replace(map_tag6_2)

    group_tag6_3 = {
        'technical issue': [
            'outdated software', 'subversion', 'rebooting', 'kernel', 'device integration', 'malfunction', 'halted', 'service restart', 'webhook', 'dataloss', 'toolset', 
            'softwareupdate', 'system upgrade', 'debugging', 'system analysis', 'software upgrade', 'data synchronization', 'gateway issue', 'connection issue', 
            'input issue', 'outdated', 'switch', 'technical', 'large dataset', 'server synchronization', 'interference', 'system alert', 'software malfunction'
        ],
        'security & privacy': [
            'containment', 'security enhancement', 'security measures', 'threat detection', 'safety measures', 'intrusion', 'suspicious activity', 
            'credential reset', 'mitigation', 'prevention', 'security breach'
        ],
        'product & docs': [
            'insights', 'report generation', 'content revision', 'modeling', 'data visualization', 'knowledge base', 'model', 'specification', 
            'workaround', 'digital tool', 'internal review', 'visual design', 'results', 'testing'
        ],
        'sales & marketing': [
            'media platforms', 'brand management', 'email campaign', 'online engagement', 'outreach', 'online marketing', 'campaign effectiveness', 
            'audience segmentation', 'content strategy', 'tactics', 'email marketing', 'brand campaigns'
        ],
        'business & finance': ['finance', 'payment options', 'financial optimization', 'cost plan', 'purchase', 'business improvement', 'cost management', 'bulk purchase', 'financial services'],
        'corporate': ['resource', 'operational', 'skill development', 'industry standards', 'project', 'hr'],
        'healthcare': ['medical', 'medical records'],
        'customer service': ['customer guidance', 'disappointment', 'platforms', 'plattform', 'login', 'service issue', 'cancellation', 'free trial', 'user guidance', 'customer value', 'technical assistance'],
        'incident': ['urgent action', 'critical incident'],
        'product & docs': ['assessment', 'evaluation', 'creativity']
    }
    map_tag6_3 = {val: key for key, values in group_tag6_3.items() for val in values}
    df['tag_6'] = df['tag_6'].replace(map_tag6_3)

    rescate_tag6 = {
        'technical issue': [
            'software integration', 'cloud native', 'data pipeline', 'microservices', 'driver', 'synchronization error', 'system synchronization', 
            'reliability', 'development', 'inconsistency', 'data source', 'system conflict', 'linux', 'softwareerror', 'partitioning', 'browser', 
            'platformupdate', 'usage', 'integration problem', 'screen sharing', 'kubernetes'
        ],
        'security & privacy': ['threat protection', 'system protection', 'outdated security', 'security incident', 'cybersecurity', 'risk exposure', 'access difficulty', 'access issue'],
        'product & docs': [
            'visual design', 'insights', 'visuals', 'recommendation', 'content revision', 'modeling', 'report generation', 'workaround', 'specification',
            'knowledge base', 'digital tool', 'internal review', 'model', 'data visualization', 'testing', 'results', 'guidelines'
        ],
        'business & finance': ['pre-sale', 'procurement', 'microsoft dynamics', 'campaigns', 'market expansion', 'investment analysis', 'reconciliation', 'financial institution', 'customer targeting', 'goals', 'ad segment'],
        'customer service': ['service packages', 'outlook account', 'e-signature', 'communication', 'third-party', 'service improvement'],
        'healthcare': ['healthcare data'],
        'incident': ['urgency'],
        'corporate': ['certification', 'misalignment', 'infrastructure management', 'awareness', 'digital initiatives']
    }
    map_rescate_tag6 = {val: key for key, values in rescate_tag6.items() for val in values}
    df['tag_6'] = df['tag_6'].replace(map_rescate_tag6)

    #------------------- tag 7
    map_tag_7_final = {
        # 1. TECHNICAL ISSUE (Bugs, Performance, Errores, IT)
        'it': 'technical issue', 'performance': 'technical issue', 'troubleshooting': 'technical issue',
        'issue': 'technical issue', 'maintenance': 'technical issue', 'optimization': 'technical issue',
        'update': 'technical issue', 'software': 'technical issue', 'error': 'technical issue',
        'compatibility': 'technical issue', 'scalability': 'technical issue', 'disruption': 'technical issue',
        'alert': 'technical issue', 'availability': 'technical issue', 'outage': 'technical issue',
        'conflict': 'technical issue', 'system issue': 'technical issue', 'critical': 'technical issue',
        'stability': 'technical issue', 'patch': 'technical issue', 'connection': 'technical issue',
        'issue resolution': 'technical issue', 'functionality': 'technical issue', 'incident': 'technical issue',
        'bug': 'technical issue', 'service restart': 'technical issue', 'reboot': 'technical issue',
        'software update': 'technical issue', 'upgrade': 'technical issue', 'inconsistency': 'technical issue',
        'system error': 'technical issue', 'failure': 'technical issue', 'fix': 'technical issue',
        'diagnostics': 'technical issue', 'restart': 'technical issue', 'system restart': 'technical issue',
        'outdated software': 'technical issue', 'vulnerability': 'technical issue', 'version': 'technical issue',
        'sluggishness': 'technical issue', 'overload': 'technical issue', 'traffic': 'technical issue',
        'load': 'technical issue', 'correction': 'technical issue', 'funktionalität': 'technical issue',
        'fehlerbehebung': 'technical issue', 'application issue': 'technical issue', 'service issue': 'technical issue',

        # 2. INFRAESTRUCTURA (Hardware, Red, Cloud, Servidores, OS)
        'recovery': 'infrastructure', 'system': 'infrastructure', 'platform': 'infrastructure',
        'configuration': 'infrastructure', 'infrastructure': 'infrastructure', 'cache': 'infrastructure',
        'hardware': 'infrastructure', 'network': 'infrastructure', 'database': 'infrastructure',
        'browser': 'infrastructure', 'firewall': 'infrastructure', 'protocol': 'infrastructure',
        'protocols': 'infrastructure', 'synchronization': 'infrastructure', 'server': 'infrastructure',
        'api': 'infrastructure', 'device': 'infrastructure', 'connectivity': 'infrastructure',
        'microservices': 'infrastructure', 'iot': 'infrastructure', 'microservice': 'infrastructure',
        'kubernetes': 'infrastructure', 'docker': 'infrastructure', 'backend': 'infrastructure',
        'gateway': 'infrastructure', 'log': 'infrastructure', 'container': 'infrastructure',
        'capacity': 'infrastructure', 'mobile': 'infrastructure', 'endpoint': 'infrastructure',

        # 3. PRODUCT & DOCS (Documentación, Feedback, UI/UX, Desarrollo)
        'integration': 'product & docs', 'feedback': 'product & docs', 'feature': 'product & docs',
        'documentation': 'product & docs', 'product': 'product & docs', 'improvement': 'product & docs',
        'workflow': 'product & docs', 'content': 'product & docs', 'implementation': 'product & docs',
        'technology': 'product & docs', 'application': 'product & docs', 'user experience': 'product & docs',
        'enhancement': 'product & docs', 'development': 'product & docs', 'notification': 'product & docs',
        'standards': 'product & docs', 'tool': 'product & docs', 'innovation': 'product & docs',
        'content creation': 'product & docs', 'automation': 'product & docs', 'instruction': 'product & docs',
        'digital tool': 'product & docs', 'code': 'product & docs', 'deployment': 'product & docs',
        'visual design': 'product & docs', 'modeling': 'product & docs', 'model': 'product & docs',
        'dashboard': 'product & docs', 'tools': 'product & docs', 'audio editing': 'product & docs',
        'user guide': 'product & docs', 'manual': 'product & docs', 'indexing': 'product & docs',
        'zapier': 'product & docs', 'smartsheet': 'product & docs', 'shopware': 'product & docs',

        # 4. SECURITY & PRIVACY (Seguridad, Datos, HIPAA, Salud)
        'healthcare': 'security & privacy', 'compliance': 'security & privacy', 'access control': 'security & privacy',
        'data integrity': 'security & privacy', 'security': 'security & privacy', 'security measures': 'security & privacy',
        'information security': 'security & privacy', 'medical data': 'security & privacy', 'privacy': 'security & privacy',
        'integrity': 'security & privacy', 'unauthorized access': 'security & privacy', 'patient data': 'security & privacy',
        'sensitive data': 'security & privacy', 'data protection': 'security & privacy', 'data security': 'security & privacy',
        'confidentiality': 'security & privacy', 'data availability': 'security & privacy', 'system integrity': 'security & privacy',
        'safety': 'security & privacy', 'monitoring': 'security & privacy', 'sensitive information': 'security & privacy',
        'risk': 'security & privacy', 'risk management': 'security & privacy', 'data': 'security & privacy',
        'data synchronization': 'security & privacy', 'data accuracy': 'security & privacy', 'data breach': 'security & privacy',
        'data integration': 'security & privacy', 'patient information': 'security & privacy', 'medical information': 'security & privacy',
        'healthcare data': 'security & privacy', 'hospital system': 'security & privacy', 'hospital systems': 'security & privacy',
        'healthcare technology': 'security & privacy', 'healthcare system': 'security & privacy', 'patient safety': 'security & privacy',
        'outdated security': 'security & privacy', 'system protection': 'security & privacy', 'encryption': 'security & privacy',
        'password reset': 'security & privacy', 'security features': 'security & privacy', 'access management': 'security & privacy',
        'patientprivacy': 'security & privacy', 'access': 'security & privacy', 'data access': 'security & privacy',
        'regulation': 'security & privacy', 'data issue': 'security & privacy', 'data management': 'security & privacy',

        # 5. BUSINESS & FINANCE (Ventas, Marketing, Estrategia, Finanzas)
        'strategy': 'business & finance', 'growth': 'business & finance', 'digital marketing': 'business & finance',
        'analytics': 'business & finance', 'brand growth': 'business & finance', 'saas': 'business & finance',
        'discount': 'business & finance', 'social media': 'business & finance', 'online presence': 'business & finance',
        'pricing': 'business & finance', 'digital': 'business & finance', 'campaign': 'business & finance',
        'sales': 'business & finance', 'brand development': 'business & finance', 'brand awareness': 'business & finance',
        'insight': 'business & finance', 'brand': 'business & finance', 'subscription': 'business & finance',
        'investment': 'business & finance', 'analysis': 'business & finance', 'target audience': 'business & finance',
        'promotion': 'business & finance', 'advertising': 'business & finance', 'cost': 'business & finance',
        'lead': 'business & finance', 'branding': 'business & finance', 'conversion': 'business & finance',
        'visibility': 'business & finance', 'agency': 'business & finance', 'expansion': 'business & finance',
        'audience': 'business & finance', 'digital campaign': 'business & finance', 'market trends': 'business & finance',
        'online advertising': 'business & finance', 'online marketing': 'business & finance', 'digital strategy': 'business & finance',
        'business expansion': 'business & finance', 'brand expansion': 'business & finance', 'influencer': 'business & finance',
        'invoice': 'business & finance', 'fees': 'business & finance', 'market': 'business & finance',
        'investment strategy': 'business & finance', 'cost breakdown': 'business & finance', 'financial data': 'business & finance',
        'financial strategy': 'business & finance', 'digital growth': 'business & finance', 'expense': 'business & finance',
        'payment plans': 'business & finance', 'payment plan': 'business & finance', 'revenue': 'business & finance',
        'price': 'business & finance', 'tariff plan': 'business & finance', 'finance': 'business & finance',
        'portfolio optimization': 'business & finance', 'cost estimation': 'business & finance', 'refund': 'business & finance',
        'offer': 'business & finance', 'invoicing': 'business & finance', 'payment': 'business & finance',

        # 6. CUSTOMER SERVICE (Soporte, Ayuda, Gestión Operativa)
        'tech support': 'customer service', 'support': 'customer service', 'assistance': 'customer service',
        'guidance': 'customer service', 'customer support': 'customer service', 'customer service': 'customer service',
        'service': 'customer service', 'effectiveness': 'customer service', 'efficiency': 'customer service',
        'technical support': 'customer service', 'best practices': 'customer service', 'project management': 'customer service',
        'investigation': 'customer service', 'account': 'customer service', 'planning': 'customer service',
        'customer engagement': 'customer service', 'engagement': 'customer service', 'decision making': 'customer service',
        'resources': 'customer service', 'process': 'customer service', 'root cause': 'customer service',
        'solution': 'customer service', 'training': 'customer service', 'inquiry': 'customer service',
        'case study': 'customer service', 'decision': 'customer service', 'certification': 'customer service',
        'delay': 'customer service', 'plan': 'customer service', 'organization': 'customer service',
        'benefits': 'customer service', 'impact': 'customer service', 'setup': 'customer service',
        'productivity': 'customer service', 'adjustment': 'customer service', 'business': 'customer service',
        'employee': 'customer service', 'quality': 'customer service', 'satisfaction': 'customer service',
        'urgent': 'customer service', 'problem-solving': 'customer service', 'management': 'customer service',
        'best practice': 'customer service', 'rapid resolution': 'customer service', 'overview': 'customer service',
        'organizational development': 'customer service', 'decision support': 'customer service', 'resource management': 'customer service',
        'consultation': 'customer service', 'reporting': 'customer service', 'operation': 'customer service',
        'resolution': 'customer service', 'progress': 'customer service', 'response time': 'customer service',
        'technicalsupport': 'customer service', 'customer loyalty': 'customer service'
    }
    df['tag_7'] = df['tag_7'].apply(lambda x: 
        x if (not isinstance(x, str) or '/' in x or x == 'none provided') 
        else map_tag_7_final.get(x.lower().strip(), x)
    )

    rescate_tag7 = {
        'business & finance': [
            'financial institution', 'financial goals', 'escalation', 'license', 'long-term commitment', 'enterprise', 'business objectives', 'budget', 
            'setup fee', 'competitive edge', 'pricing information', 'frequency', 'investment decision', 'market expansion', 'ad spend', 'market', 
            'market trends', 'campaigns', 'behavior', 'audit', 'billing', 'transaction', 'challenges', 'payment system'
        ],
        'product & docs': [
            'real-time analytics', 'platform compatibility', 'segmentation', 'platform insight', 'platform compatibility', 'platform insight', 'digital presence', 
            'diagnostic', 'real-time insight', 'predictive', 'report', 'refinement', 'tactics', 'insights', 'advice', 'large dataset', 'content variation', 'website',
            'recommendations', 'recommendation', 'analysis method', 'website traffic', 'whitepaper', 'advanced tools', 'outcome', 'good practice', 'fine tuning', 
            'messaging', 'app', 'digital platforms','digital transformation', 'behaviour', 'analyse'
        ],
        'security & privacy': [
            'wi-fi security', 'safety measure', 'security vulnerability', 'risk assessment', 'threatprevention', 'confidential data', 'firewall assessment',
            'securityaudit', 'sensitive resource', 'external attack', 'data damage', 'security solution'
        ],
        'technical issue': [
            'modern technology', 'browser cache', 'sap erp', 'issue resolution', 'system investigation', 'system integration', 'installation', 
            'system compatibility', 'calibration', 'service log', 'alarm', 'system inconsistency', 'tool support', 'system compatibility', 'system issue'
        ],
        'customer service': [
            'status update', 'process challenge', 'process improvement', 'free trial', 'scan', 'disappointment', 'kunde', 'response', 'relationship', 'discussion', 'understanding', 
            'support inquiry', 'hospital management', 'customer support', 'support available', 'partnerships', 'consultation', 'trial', 'service request', 'community', 'response','user', 'patient'
        ],
        'corporate': ['organizational requirements', 'plans', 'resource bottleneck', 'data processing', 'patient', 'data processing', 'medicaldata', 'hospital management', 'hospital solution','collaboration']
    }
    map_rescate_tag7 = {val: key for key, values in rescate_tag7.items() for val in values}
    df['tag_7'] = df['tag_7'].replace(map_rescate_tag7)



    #------------------- tag 8
    map_tag_8 = {
        # 1. TECHNICAL ISSUE (Bugs, Performance, Errores, IT)
        'it': 'technical issue', 'performance': 'technical issue', 'issue': 'technical issue',
        'update': 'technical issue', 'troubleshooting': 'technical issue', 'error': 'technical issue',
        'scalability': 'technical issue', 'software': 'technical issue', 'software update': 'technical issue',
        'availability': 'technical issue', 'resolution': 'technical issue', 'alert': 'technical issue',
        'incident': 'technical issue', 'connection': 'technical issue', 'bug': 'technical issue',
        'discrepancy': 'technical issue', 'disruption': 'technical issue', 'data overload': 'technical issue',
        'issue analysis': 'technical issue', 'error resolution': 'technical issue', 'system problem': 'technical issue',
        'systemslowdown': 'technical issue', 'error fix': 'technical issue', 'functionality': 'technical issue',
        'integration error': 'technical issue', 'optimization': 'technical issue', 'compatibility': 'technical issue',
        'problem': 'technical issue', 'system availability': 'technical issue', 'user troubleshooting': 'technical issue',
        'algorithm': 'technical issue', 'downgrade': 'technical issue',
        # 2. INFRAESTRUCTURA (Hardware, Red, Cloud, Servidores, OS)
        'platform': 'infrastructure', 'cache': 'infrastructure', 'system': 'infrastructure',
        'firmware': 'infrastructure', 'infrastructure': 'infrastructure', 'configuration': 'infrastructure',
        'service breakdown': 'infrastructure', 'cache clear': 'infrastructure', 'protocol': 'infrastructure',
        'backup': 'infrastructure', 'caching': 'infrastructure', 'wi-fi': 'infrastructure',
        'network': 'infrastructure', 'database': 'infrastructure', 'mysql': 'infrastructure',
        'hardware': 'infrastructure', 'recovery': 'infrastructure', 'vr headset': 'infrastructure',
        # 3. PRODUCT & DOCS (Documentación, Feedback, UI/UX, Desarrollo)
        'documentation': 'product & docs', 'feedback': 'product & docs', 'feature': 'product & docs',
        'features': 'product & docs', 'integration': 'product & docs', 'implementation': 'product & docs',
        'improvement': 'product & docs', 'technology': 'product & docs', 'tool': 'product & docs',
        'digital tools': 'product & docs', 'application': 'product & docs', 'user experience': 'product & docs',
        'data synchronization': 'product & docs', 'data format': 'product & docs', 'tutorial': 'product & docs',
        'dashboard': 'product & docs', 'digital integration': 'product & docs', 'platform integration': 'product & docs',
        'technical tools': 'product & docs', 'customization': 'product & docs', 'workflow': 'product & docs',
        'content': 'product & docs', 'standards': 'product & docs', 'setup': 'product & docs',
        'verification': 'product & docs',
        # 4. SECURITY & PRIVACY (Seguridad, Datos, HIPAA, Salud)
        'security': 'security & privacy', 'healthcare': 'security & privacy', 'confidentiality': 'security & privacy',
        'hospital': 'security & privacy', 'medical data': 'security & privacy', 'sensitive data': 'security & privacy',
        'data': 'security & privacy', 'protection': 'security & privacy', 'integrity': 'security & privacy',
        'data availability': 'security & privacy', 'patient data': 'security & privacy', 'patient safety': 'security & privacy',
        'sensitive': 'security & privacy', 'access restriction': 'security & privacy', 'emr': 'security & privacy',
        'data encryption': 'security & privacy', 'patient privacy': 'security & privacy', 'access control': 'security & privacy',
        'vulnerability': 'security & privacy', 'system security': 'security & privacy', 'information security': 'security & privacy',
        'medicalinformation': 'security & privacy', 'security solutions': 'security & privacy', 'security update': 'security & privacy',
        'compliance': 'security & privacy', 'privacy': 'security & privacy', 'regulations': 'security & privacy',
        'credential': 'security & privacy',
        # 5. BUSINESS & FINANCE (Ventas, Marketing, Estrategia, Finanzas)
        'strategy': 'business & finance', 'growth': 'business & finance', 'digital marketing': 'business & finance',
        'analytics': 'business & finance', 'brand growth': 'business & finance', 'saas': 'business & finance',
        'discount': 'business & finance', 'online presence': 'business & finance', 'agency': 'business & finance',
        'brand development': 'business & finance', 'branding': 'business & finance', 'brand': 'business & finance',
        'promotion': 'business & finance', 'campaign': 'business & finance', 'billing': 'business & finance',
        'social media': 'business & finance', 'business expansion': 'business & finance', 'brand awareness': 'business & finance',
        'financial': 'business & finance', 'online initiative': 'business & finance', 'marketing': 'business & finance',
        'credit card': 'business & finance', 'ongoing charge': 'business & finance', 'billing period': 'business & finance',
        'online advertising': 'business & finance', 'revenue': 'business & finance', 'data analytics': 'business & finance',
        'product portfolio': 'business & finance', 'predictive analytics': 'business & finance', 'tactics': 'business & finance',
        'fees': 'business & finance', 'cost analysis': 'business & finance', 'business development': 'business & finance',
        'business growth': 'business & finance', 'advertising': 'business & finance', 'financial issue': 'business & finance',
        'financial organization': 'business & finance', 'pricing': 'business & finance', 'high return': 'business & finance',
        'revenue': 'business & finance', 'website': 'business & finance', 'search engine': 'business & finance',
        'digital': 'business & finance', 'visibility': 'business & finance', 'brand visibility': 'business & finance',
        'insight': 'business & finance', 'insights': 'business & finance', 'trend': 'business & finance',
        # 6. CUSTOMER SERVICE (Soporte, Ayuda, Gestión Operativa)
        'tech support': 'customer service', 'support': 'customer service', 'assistance': 'customer service',
        'guidance': 'customer service', 'customer support': 'customer service', 'customer service': 'customer service',
        'service': 'customer service', 'technical support': 'customer service', 'best practices': 'customer service',
        'best practice': 'customer service', 'recommendation': 'customer service', 'effectiveness': 'customer service',
        'efficiency': 'customer service', 'project management': 'customer service', 'engagement': 'customer service',
        'case study': 'customer service', 'account': 'customer service', 'resources': 'customer service',
        'resource': 'customer service', 'decision': 'customer service', 'response': 'customer service',
        'solution': 'customer service', 'urgent': 'customer service', 'information': 'customer service',
        'customer': 'customer service', 'success': 'customer service', 'timely': 'customer service',
        'effort': 'customer service', 'email': 'customer service', 'adjustment': 'customer service',
        'techniques': 'customer service', 'progress': 'customer service', 'root cause': 'customer service',
        'decision making': 'customer service', 'review': 'customer service', 'consultation': 'customer service',
        'audit': 'customer service', 'communication': 'customer service', 'investigation': 'customer service',
        'research': 'customer service', 'success story': 'customer service', 'advice': 'customer service',
        'customer inquiry': 'customer service', 'reporting': 'customer service', 'technique': 'customer service',
        'process': 'customer service', 'operation': 'customer service', 'organization': 'customer service',
        'team collaboration': 'customer service', 'team management': 'customer service', 'assessment': 'customer service',
        'training': 'customer service', 'opportunity': 'customer service', 'product advice': 'customer service',
        'tool support': 'customer service', 'trial': 'customer service', 'timeline': 'customer service'
    }
    df['tag_8'] = df['tag_8'].apply(lambda x:
        x if (not isinstance(x, str) or ',' in x or '/' in x or x == 'not provided')
        else map_tag_8.get(x.lower().strip(), x)
    )


    #----------------------- 4. Re name y re orden -----------------------
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
    df['dataset_source'] = 'multi_lang_5'

    return df