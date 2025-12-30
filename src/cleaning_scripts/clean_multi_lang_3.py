import pandas as pd

def clean_multi_lang_3(filepath):
    #ver colab
    #https://colab.research.google.com/drive/1X19xsNd5w56P9wBA1d4L2DZ9AfbwsOVh#scrollTo=UM7kBZ9QGveh

    
    df = pd.read_csv(filepath)

    #---------------------------     1. Limpieza y estandarizacion    -------------------------------
    cols_to_drop = ['tag_9']
    df = df.drop(columns=cols_to_drop, errors='ignore')    


    cols_to_fix = [column for column in df.columns]
    
    for column in cols_to_fix:
        if df[column].dtype == 'object':  #Solo si es tipo texto
            df[column] = (df[column]
                       .str.lower()       #Todo a minúsculas
                       .str.strip()       #Quitar espacios al inicio/final
                       .str.replace(r'\s+', ' ', regex=True)) #Quitar espacios dobles
            

    df = df.dropna(subset=['body']) #ahora son 3999 registros


    values_to_fill = {
        'subject': 'no matter',
    }
    df = df.fillna(value=values_to_fill)


    tag_columns = [col for col in df.columns if col.startswith('tag_')]
    df[tag_columns] = df[tag_columns].fillna('not provided')




    #---------------------      2. Mapeo de datos y validacion    ---------------------
    expected_types = ['incident', 'request', 'problem','change']    
    df = df[df['type'].isin(expected_types)]


    expected_languages = ['en', 'de', 'es', 'fr', 'pt']
    df = df[df['language'].isin(expected_languages)]    


    split_data = df['queue'].str.split('/', n=1, expand=True)
    df['sector'] = split_data[0].str.strip()
    if split_data.shape[1] > 1:
        df['specific_sector'] = split_data[1].fillna("not provided").str.strip()
    else:
        df['specific_sector'] = "not provided"


    business_type_mapping = {
            'it services': 'it services','_it_services_': 'it services','pit services': 'it services', #P es error de tipeo
            'it consulting firm': 'it consulting','it consulting service': 'it consulting',
            'tech online store': 'online store','online store': 'online store',
            'software development company': 'software development','adobe photoshop 2024': 'software development'#tipo de software
        }
    df['business_type'] = df['business_type'].map(business_type_mapping).fillna('other')



    #----------------------     3. Transformacion       --------------------------
    priority_map = { 'low': 2, 'medium': 3, 'high': 4 }
    df['priority_level'] = df['priority'].map(priority_map)

    lang_map = {
        'en': 'english', 'de': 'german', 'es': 'spanish', 'fr': 'french', 'pt': 'portuguese'
    }
    df['language'] = df['language'].map(lang_map).fillna('unknown')

        
    #estos mapeos aqui porque hay varios valores transformados y no solo estandarizados
    #tag 1
    tag1_map = {
        "return and exchanges": "returns and exchanges", "product replacement": "returns and exchanges",
        "shipping delay": "shipment delay", "delivery issue": "shipment delay", "order issue": "shipment delay",
        "password reset": "login issue", "account security": "login issue", "account assistance": "login issue",
        "data breach": "login issue", "critical failure": "urgent issue", "emergency issue": "urgent issue",
        "incident report": "urgent issue", "system maintenance": "it support", "service maintenance": "it support",
        "system crash": "it support", "database issue": "it support", "hardware failure": "it support",
        "performance tuning": "it support", "payment processing": "billing issue", "refund request": "billing issue",
        "technical guidance": "technical support", "customer feedback": "customer service", 
        "general inquiry": "customer service", "routine request": "customer service",
        "consulting services": "sales inquiry", "feature request": "product support"          
    }
    df['tag_1'] = df['tag_1'].str.strip()
    df['tag_1'] = df['tag_1'].map(tag1_map).fillna(df['tag_1'])
    

    #tag 2
    tag2_map = {
        # 1. Technical Support & Software
        'technical support': 'technical support', 'software bug': 'technical support', 'problem resolution': 'technical support',
        'performance tuning': 'technical support', 'technical guidance': 'technical support', 'feature request': 'technical support',
        'troubleshooting': 'technical support', 'documentation request': 'technical support', 'system maintenance': 'technical support',
        # 2. Hardware & Infrastructure
        'hardware failure': 'hardware & infrastructure', 'printer issue': 'hardware & infrastructure', 'printing issue': 'hardware & infrastructure',
        'printer setup': 'hardware & infrastructure', 'laptop issue': 'hardware & infrastructure', 'audio issue': 'hardware & infrastructure',
        # 3. IT & Cloud Services
        'it support': 'it services', 'cloud services': 'it services', 'cloud migration': 'it services', 'aws management': 'it services',
        'aws management service': 'it services', 'backup restore': 'it services', 'consulting services': 'it services', 'consultation request': 'it services',
        # 4. Networking & Connectivity
        'network issue': 'networking', 'wireless setup': 'networking', 'wi-fi issue': 'networking', 'wi-fi connectivity issue': 'networking',
        'wireless connectivity': 'networking', 'connectivity problem': 'networking', 'networking issue': 'networking',
        # 5. Service Incidents (Outages/Crashes)
        'service disruption': 'service incidents', 'system crash': 'service incidents', 'incident report': 'service incidents',
        'service recovery': 'service incidents', 'service outage': 'service incidents', 'service notification': 'service incidents', 'planned change': 'service incidents',
        # 6. Database Management
        'database issue': 'database management', 'database support': 'database management', 'database connection': 'database management', 'database optimization': 'database management',
        # 7. Account & Security
        'account assistance': 'account & security', 'login issue': 'account & security', 'account security': 'account & security',
        'data breach': 'account & security', 'email access': 'account & security', 'email configuration': 'account & security', 'virus removal': 'account & security',
        # 8. Billing & Payments
        'payment processing': 'billing & payments', 'refund request': 'billing & payments', 'billing issue': 'billing & payments',
        'cost optimization': 'billing & payments', 'cost optimization assistance': 'billing & payments', 'budget management': 'billing & payments', 'accounting assistance': 'billing & payments',
        # 9. Customer Service & Sales
        'customer service': 'customer service', 'product support': 'customer service', 'general inquiry': 'customer service',
        'customer feedback': 'customer service', 'sales inquiry': 'customer service', 'routine request': 'customer service',
        # 10. Logistics & Returns
        'returns and exchanges': 'logistics & returns', 'warranty claim': 'logistics & returns', 'order issue': 'logistics & returns',
        'shipment delay': 'logistics & returns', 'product replacement': 'logistics & returns', 'shipping options': 'logistics & returns',
        # 11. Critical & HR
        'urgent issue': 'urgent/critical', 'critical failure': 'urgent/critical', 'human resources': 'human resources',
        'employee onboarding': 'human resources', 'employee inquiry': 'human resources', 'training request': 'human resources'
    }
    df['tag_2'] = df['tag_2'].str.strip()
    df['tag_2'] = df['tag_2'].map(tag2_map).fillna(df['tag_2'])


    # tag 3
    tag3_homologation_map = {
        'wi-fi connectivity issue': 'network issue', 'wi-fi issue': 'network issue', 
        'wireless connectivity': 'network issue', 'connectivity problem': 'network issue',
        'networking issue': 'network issue', 'wireless setup': 'network issue',
        'printing issue': 'printer issue', 'printer setup': 'printer issue',
        'database support': 'database issue', 'database connection': 'database issue',
        'database optimization': 'database issue', 'aws management': 'cloud services',
        'aws management service': 'cloud services', 'cloud migration': 'cloud services',
        'cost optimization assistance': 'cost optimization', 'consultation request': 'consulting services',
        'laptop issue': 'hardware failure', 'audio issue': 'hardware failure',
        'shipping options': 'logistics', 'shipment delay': 'logistics'
    }
    df['tag_3'] = df['tag_3'].str.strip()
    df['tag_3'] = df['tag_3'].map(tag3_homologation_map).fillna(df['tag_3'])

    tag3_unify_map = {
        # Redes y Conectividad
        'wi-fi connectivity issue': 'network issue', 'wi-fi issue': 'network issue', 'wireless connectivity': 'network issue', 
        'connectivity problem': 'network issue', 'networking issue': 'network issue', 'wireless setup': 'network issue',
        # Hardware e Infraestructura
        'printer issue': 'hardware & infrastructure', 'printing issue': 'hardware & infrastructure', 'printer setup': 'hardware & infrastructure',
        'laptop issue': 'hardware & infrastructure', 'audio issue': 'hardware & infrastructure', 'hardware failure': 'hardware & infrastructure',
        # IT Services & Cloud
        'aws management': 'it services', 'aws management service': 'it services', 'cloud migration': 'it services', 
        'cloud services': 'it services', 'database issue': 'it services', 'database support': 'it services', 
        'database connection': 'it services', 'database optimization': 'it services', 'server maintenance': 'it services',
        # Soporte Técnico y Software
        'software bug': 'technical support', 'system crash': 'technical support', 'virus removal': 'technical support',
        'troubleshooting': 'technical support', 'email configuration': 'technical support', 'email access': 'technical support',
        # Cliente y Logística
        'shipment delay': 'logistics & returns', 'shipping options': 'logistics & returns', 'returns and exchanges': 'logistics & returns',
        'refund request': 'logistics & returns', 'product replacement': 'logistics & returns', 'warranty claim': 'logistics & returns',
        # Administración y RRHH
        'human resources': 'hr & admin', 'employee onboarding': 'hr & admin', 'employee inquiry': 'hr & admin',
        'payroll issue': 'hr & admin', 'budget management': 'hr & admin', 'accounting assistance': 'hr & admin'
    }
    df['tag_3'] = df['tag_3'].str.strip().replace(tag3_unify_map)
 

    #tag_4
    tag4_map = {
        # Video Conferencing
        'video conference': 'video conferencing', 'video conferencing issue': 'video conferencing',
        # Wi-Fi / Redes
        'wi-fi configuration': 'network issue', 'wi-fi setup': 'network issue', 'wi-fi connectivity': 'network issue',
        'wireless configuration': 'network issue', 'connectivity issues': 'network issue', 'connectivity problem': 'network issue',
        # Impresoras
        'print issue': 'printer issue', 'printing issue': 'printer issue', 'printer issues': 'printer issue', 'printer connectivity': 'printer issue', 'mobile printing': 'printer issue',
        # Cloud & AWS
        'aws management service': 'cloud services', 'aws support': 'cloud services', 'cloud optimization': 'cloud services',
        'cloud architecture': 'cloud services', 'cloud deployment': 'cloud services', 'cloud support': 'cloud services',
        # Otros duplicados
        'firmware upgrade': 'firmware update', 'database connection issue': 'database issue', 'bulk purchase inquiry': 'sales inquiry'
    }
    df['tag_4'] = df['tag_4'].str.strip().str.lower().replace(tag4_map)

    tag4_rescate_map = {
        # Hacia Technical Support / IT Support
        'troubleshooting': 'technical support', 'help request': 'technical support', 'systems maintenance': 'system maintenance', 'server management': 'it support',
        'software development': 'it support', 'password reset': 'it support', 'office 365': 'it support', 'authentication issue': 'it support',
        'backup restore': 'it support', 'virus removal': 'technical support', 'firmware update': 'technical support',
        # Hacia Hardware / Infrastructure
        'battery issue': 'hardware failure', 'audio issues': 'hardware failure', 'printer issue': 'hardware failure',
        # Hacia Network / Wireless
        'wireless connectivity': 'network issue', 'vpn issue': 'network issue',
        # Hacia Customer Service / Business
        'billing issue': 'payment processing', 'invoice request': 'payment processing', 'shipping inquiry': 'order issue', 'return policy': 'returns and exchanges',
        'cost management': 'cost optimization', 'human resources': 'customer service', 'employee inquiry': 'customer service', 'employee onboarding': 'customer service',
        # Hacia Critical / Urgent
        'service outage': 'urgent issue', 'critical failure': 'urgent issue', 'system disruption': 'service disruption',
        # Hacia Consulting / Inquiry
        'consultation request': 'consulting services', 'consultation process': 'consulting services', 'not provided': 'general inquiry'
    }
    df['tag_4'] = df['tag_4'].str.strip().replace(tag4_rescate_map)


    #tag5
    tag5_map = {
        # Agrupación de Cloud/AWS (Varios con frecuencia 1)
        'amazon web services': 'cloud services', 'aws assistance': 'cloud services', 'aws optimization': 'cloud services', 'cloud computing': 'cloud services',
        'cloud solutions': 'cloud services', 'cloud infrastructure': 'cloud services',
        # Agrupación de Conectividad (Frecuencia 1)
        'wi-fi connection': 'network issue', 'wi-fi issues': 'network issue', 'wi-fi issue': 'network issue', 'internet connectivity': 'network issue',
        'connectivity issue': 'network issue', 'networking issue': 'network issue', 'wireless setup': 'network issue', 'router configuration': 'network issue',
        # Agrupación de Software/Sistemas
        'email issues': 'technical support', 'email issue': 'technical support', 'email configuration': 'technical support', 'browser issue': 'technical support',
        'microsoft excel': 'technical support', 'notification issue': 'technical support', 'system optimization': 'technical support', 'systems maintenance': 'system maintenance',
        # Agrupación de Hardware
        'battery performance': 'hardware failure', 'overheating issue': 'hardware failure',
        # Administrativos
        'document request': 'documentation request', 'shipping delay': 'order issue', 'shipment delay': 'order issue', 'bulk purchase': 'sales inquiry',
        'corporate purchasing': 'sales inquiry', 'exchange process': 'returns and exchanges'
    }
    df['tag_5'] = df['tag_5'].str.strip().str.lower().replace(tag5_map)

    tag5_unification_map = {
        # Hacia Logística/Ordenes
        'shipping inquiry': 'order issue', 'user setup': 'account assistance',
        # Hacia Cloud/IT Support
        'aws support': 'cloud services', 'server setup': 'it support', 'server configuration': 'it support', 'backup restore': 'system maintenance',
        'virus removal': 'system maintenance', 'troubleshooting': 'technical support', 'security patch': 'system maintenance', 'integration assistance': 'technical support',
        # Hacia HR/General Inquiry
        'employee inquiry': 'general inquiry', 'employee onboarding': 'general inquiry',  'user guidance': 'general inquiry', 'consultation process': 'consulting services',
        # Hacia Management/Planning
        'cost management': 'cost optimization', 'financial impact': 'cost optimization', 'project management': 'planned change',
        'process improvement': 'planned change', 'compliance issue': 'planned change', 'service outage': 'service disruption',
        # Temas Específicos que pueden ir a sus padres
        'printer issue': 'hardware failure', 'customer feedback': 'customer service', 'system disruption': 'service disruption',
    }
    df['tag_5'] = df['tag_5'].replace(tag5_unification_map)


    #tag6
    tag6_map = {
    # Hacia IT & System Support
        'systems maintenance': 'system maintenance', 'backup restore': 'system maintenance','virus removal': 'system maintenance', 'firmware update': 'system maintenance',
        'software application': 'it support', 'application issue': 'it support','database support': 'it support', 'database optimization': 'it support',
        'database issue': 'it support', 'server issue': 'it support','data migration': 'it support', 'troubleshooting': 'technical support',
        'installation issues': 'technical support', 'configuration change': 'it support','configuration change request': 'it support',
        'modification request': 'it support','change request': 'it support',
        # Hacia Cloud & Network
        'cloud management': 'cloud services', 'cloud computing': 'cloud services','cloud issues': 'cloud services', 'aws management': 'cloud services',
        'aws support': 'cloud services', 'router issue': 'network issue','router issues': 'network issue', 'routing request': 'network issue',
        # Hacia Seguridad y Riesgo
        'security vulnerability': 'security', 'security improvement': 'security','security enhancement': 'security', 'data breach': 'security','account security': 'security',
        # Hacia Hardware
        'battery issue': 'hardware failure', 'printer issue': 'hardware failure','device issues': 'hardware failure', 'audio issues': 'hardware failure',
        # Hacia Business & Logistics
        'corporate purchasing': 'sales inquiry', 'pricing information': 'sales inquiry','corporate account': 'sales inquiry', 'shipping instructions': 'order issue',
        'shipment delay': 'order issue', 'exchange request': 'returns and exchanges','billing issue': 'payment processing', 'cost management': 'cost optimization',
        'document request': 'documentation request', 'maintenance tips': 'technical guidance',
        # Hacia HR & Admin
        'employee inquiry': 'general inquiry', 'employee onboarding': 'general inquiry','administration issue': 'general inquiry',
        # Hacia Critical
        'service outage': 'urgent issue', 'outage alert': 'urgent issue'
    }
    df['tag_6'] = df['tag_6'].str.strip().replace(tag6_map)


    #tag7
    tag7_map = {
        # Hacia Cloud & Infraestructura
        'aws implementation': 'cloud services', 'aws management service': 'cloud services','aws support': 'cloud services', 'aws service inquiry': 'cloud services',
        'cloud computing': 'cloud services', 'cloud infrastructure': 'cloud services','infrastructure management': 'it support', 'server administration': 'it support',
        # Hacia Redes y Conectividad
        'wifi connectivity': 'network issue', 'wi-fi connectivity': 'network issue','connectivity issues': 'network issue',
        # Hacia Seguridad
        'security vulnerability': 'security', 'security update': 'security','data breach': 'security', 'account security': 'security',
        # Hacia Hardware y Soporte Técnico
        'printer issue': 'hardware failure', 'cooling issues': 'hardware failure','audio issues': 'hardware failure', 'firmware update': 'technical support',
        'email setup': 'technical support', 'password reset': 'it support','configuration help': 'technical support', 'video conference support': 'technical support',
        # Hacia Negocio y Ventas
        'order inquiry': 'order issue', 'invoice inquiry': 'billing issue','purchase request': 'sales inquiry', 'cost management': 'cost optimization','returns and exchanges': 'logistics', 'shipping delay': 'logistics'
    }
    df['tag_7'] = df['tag_7'].str.strip().replace(tag7_map)

    tag7_rescate = {
        # Hacia IT & Support (Consolidando los '1' y '2')
        'database support': 'it support', 'database issue': 'it support', 
        'data migration': 'it support', 'login issue': 'it support',
        'technical support': 'it support', 'software bug': 'it support',
        # Hacia Business & Admin
        'consultancy services': 'consulting services', 'consultation request': 'consulting services',
        'project management': 'planned change', 'training request': 'documentation request',
        'billing issue': 'payment processing', 'sales inquiry': 'order issue',
        # Hacia Service Management (Unificando los críticos)
        'critical failure': 'urgent issue', 'system crash': 'service disruption',
        'service disruption': 'urgent issue'
    }
    df['tag_7'] = df['tag_7'].str.strip().replace(tag7_rescate)


    #tag8
    tag8_map = {
        # Hacia IT & Soporte Técnico
        'firmware upgrade': 'system maintenance', 'firmware update': 'system maintenance','backup restore': 'system maintenance', 'data backup solutions': 'system maintenance',
        'database optimization': 'it support', 'database issue': 'it support','system configuration': 'it support', 'configuration issue': 'it support',
        'system optimization': 'performance tuning', 'optimization request': 'performance tuning','access issues': 'it support', 'creative cloud': 'software bug',
        # Hacia Cloud & Redes
        'aws configuration': 'cloud services', 'ink consumption issue': 'hardware failure',
        # Hacia Consultoría y Business
        'consultation request': 'consulting services', 'consulting service': 'consulting services','architectural planning': 'consulting services', 'financial management': 'cost optimization',
        'discount request': 'sales inquiry', 'information request': 'general inquiry','customer support': 'customer service', 'billing issue': 'payment processing','return and exchanges': 'warranty claim'
    }
    df['tag_8'] = df['tag_8'].str.strip().replace(tag8_map)

    tag8_rescate = {
        # Unificar categorías con frecuencia < 4 hacia sus padres fuertes
        'security vulnerability': 'it support', 'payment processing': 'customer service','technical support': 'it support','cost optimization': 'general inquiry',
        'sales inquiry': 'customer service','routine request': 'general inquiry','customer feedback': 'customer service','hardware failure': 'it support',
        'system crash': 'service disruption','cloud services': 'it support',
        # Unificar los "críticos" (6 registros) con Urgent Issue (11 registros)
        'critical failure': 'urgent issue'
    }
    df['tag_8'] = df['tag_8'].str.strip().replace(tag8_rescate)



    #------------------------------- 4. Re nombre y re orden --------------------------------
    type_rename = {'type': 'ticket_type'}
    df = df.rename(columns=type_rename)

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
        'subject','body','answer', 'ticket_type', 'queue', 'sector', 'specific_sector',
        'priority', 'priority_level', 'language', 'business_type',
        'problem_category', 'issue_classification', 'issue_subcategory', 'issue_detail', 'resolution_category',
        'resolution_subcategory', 'resolution_detail', 'additional_tag'
    ]
    df = df[column_order]


    # Identificador de origen
    df['dataset_source'] = 'multi_lang_3'
    
    return df