from neo4j import GraphDatabase
from graphviz import Digraph

### begin config
# connection to Neo4J database
driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "1234"))


##### colors
#c81919 - dark red
#f9cccc - light red
#
#0333a3 - dark blue
#bbd1ff - light blue
#
#feb729 - yellow
#fed47f - light yellow
#
#178544 - dark green
#4ae087 - light green
#
#a034a8 - purple
#e7bdeb - light purple
#
#13857d - dark cyan
#19b1a7 - cyan
#93f0ea - light cyan

c2_cyan = "#318599"
c2_orange = "#ea700d"
c2_light_orange = "#f59d56"
c2_light_yellow = "#ffd965"

c3_light_blue = "#5b9bd5"
c3_red = "#ff0000"
c3_green = "#70ad47"
c3_yellow = "#ffc000"


c4_red = '#d7191c'
c4_orange = '#fdae61'
c4_yellow = '#ffffbf'
c4_light_blue = '#abd9e9'
c4_dark_blue = '#2c7bb6'

c_white = "#ffffff"
c_black = "#000000"

c5_red = '#d73027'
c5_orange = '#fc8d59'
c5_yellow = '#fee090'
c5_light_blue = '#e0f3f8'
c5_medium_blue = '#91bfdb'
c5_dark_blue = '#4575b4'


# all cases


#case_selector = "true"

# 6 randomly selected cases showing variety of behavior
cases = ['Application_681547497',
         'Application_55972649',
         'Application_430577010',
         'Application_889180637',
         'Application_1020381296',
         'Application_1465025013',
         'Application_2014483796'
         ]
case_selector = "e1.case IN "+str(cases)


# just a single case: Application_681547497, 2 parallel offers
#case_selector = "e1.case IN ['Application_681547497']"

# just a single case: Application_55972649, resources do not explain hand-over
#case_selector = "e1.case IN ['Application_681547497','Application_2014483796']"

def getNodeLabel_Event(name):
    return name[2:7]

def getEventsDF(tx, dot, entity_type, color, fontcolor, edge_width, show_lifecycle):
    q = f'''
        MATCH (e1:Event) -[:E_EN]-> (n:Entity{{EntityType:"{entity_type}"}}) WHERE {case_selector}
        OPTIONAL MATCH (e1) -[df:DF_{entity_type}]-> (e2:Event)
        RETURN e1,df,e2
        '''
    print(q)
    
    dot.attr("node",shape="circle",fixedsize="true", width="0.4", height="0.4", fontname="Helvetica", fontsize="8", margin="0")
    for record in tx.run(q):
        
        if show_lifecycle == True:
            e1_name = getNodeLabel_Event(record["e1"]["Activity"])+'\n'+record["e1"]["lifecycle"][0:5]
        else:
            e1_name = getNodeLabel_Event(record["e1"]["Activity"])
            
        dot.node(str(record["e1"].id), e1_name, color=color, style="filled", fillcolor=color, fontcolor=fontcolor)
        
        if record["e2"] != None:
            edge_label = ""
            xlabel = ""
            pen_width = str(edge_width)
            edge_color = color
                
            dot.edge(str(record["e1"].id),str(record["e2"].id),label=edge_label,color=edge_color,penwidth=pen_width,xlabel=xlabel,fontname="Helvetica", fontsize="8",fontcolor=edge_color)
            
def getDF(tx, dot, entity_type, color, fontcolor, edge_width):
    q = f'''
        MATCH (e1:Event) -[:E_EN]-> (n:Entity{{EntityType:"{entity_type}"}}) WHERE {case_selector}
        MATCH (e1) -[df:DF_{entity_type}]-> (e2:Event)
        RETURN distinct e1,df,e2
        '''
    print(q)
    
    dot.attr("node",shape="circle",fixedsize="true", width="0.4", height="0.4", fontname="Helvetica", fontsize="8", margin="0")
    for record in tx.run(q):
        
        edge_label = ""
        xlabel = record["df"].type[len("DF_Case_"):]
        pen_width = str(edge_width)
        edge_color = color
            
        dot.edge(str(record["e1"].id),str(record["e2"].id),label=edge_label,color=edge_color,penwidth=pen_width,xlabel=xlabel,fontname="Helvetica", fontsize="8",fontcolor=edge_color)

def getResourcesWithinCaseDF(tx, dot):
    q = f'''
        match (e1:Event) -[df:DF_Case_R]-> (e2:Event) -[:E_EN]-> (r:Entity {{EntityType:"Case_R"}})
        WHERE e1.case = e2.case AND {case_selector}
        return e1,df,e2,r
        '''
    for record in tx.run(q):
        
        edge_label = ""
        xlabel = record["r"]["ID"]

        pen_width = "1"
        edge_color = c5_red
        dot.node(str(record["e1"].id),color=c5_red,penwidth="2")
        dot.node(str(record["e2"].id),color=c5_red,penwidth="2")
        dot.edge(str(record["e1"].id),str(record["e2"].id),label=edge_label,color=edge_color,penwidth=pen_width,xlabel=xlabel,fontname="Helvetica", fontsize="8",fontcolor=edge_color)

def getResourcesDF(tx, dot, edge_width):
    q = f'''
        match (r:Entity {{EntityType:"Case_R"}}) <-[:E_EN]- (e1:Event) -[df:DF_Case_R]-> (e2:Event)
        return e1,df,e2,r
        '''
    for record in tx.run(q):
        
        edge_label = ""
        xlabel = record["r"]["ID"]

        pen_width = str(edge_width)
        edge_color = c5_red
        dot.node(str(record["e1"].id),color=c5_red,penwidth="2")
        dot.node(str(record["e2"].id),color=c5_red,penwidth="2")
        dot.edge(str(record["e1"].id),str(record["e2"].id),label=edge_label,color=edge_color,penwidth=pen_width,xlabel=xlabel,fontname="Helvetica", fontsize="8",fontcolor=edge_color)

def getResourcesAcrossApplicationsDF(tx, dot):
    q = f'''
        match (e1:Event) -[df:DF_Case_R]-> (e2:Event) -[:E_EN]-> (r:Entity {{EntityType:"Case_R"}})
        where (e1)-[:E_EN]->({{EntityType:"Application"}}) and (e2)-[:E_EN]->({{EntityType:"Application"}})
        return e1,df,e2,r
        '''
    for record in tx.run(q):
        
        edge_label = ""
        xlabel = record["r"]["ID"]

        pen_width = "1"
        edge_color = c5_red
        dot.node(str(record["e1"].id),color=c5_red,penwidth="2")
        dot.node(str(record["e2"].id),color=c5_red,penwidth="2")
        dot.edge(str(record["e1"].id),str(record["e2"].id),label=edge_label,color=edge_color,penwidth=pen_width,xlabel=xlabel,fontname="Helvetica", fontsize="8",fontcolor=edge_color)
        
def getEntityForFirstEvent(tx,dot,entity_type,color,fontcolor):
    q = f'''
        MATCH (e1:Event) -[corr:E_EN]-> (n:Entity)
        WHERE n.EntityType = "{entity_type}" AND NOT (:Event)-[:DF_{entity_type}]->(e1) AND {case_selector}
        return e1,corr,n
        '''
    print(q)

    dot.attr("node",shape="rectangle",fixedsize="false", width="0.4", height="0.4", fontname="Helvetica", fontsize="8", margin="0")
    for record in tx.run(q):
        e_id = str(record["e1"].id)
        #e_name = getNodeLabel_Event(record["e"]["Activity"])
        entity_type = record["n"]["EntityType"]
        
        entity_id = record["n"]["ID"]
        entity_uid = record["n"]["uID"]
        entity_label = entity_type+'\n'+entity_id
        
        dot.node(entity_uid, entity_label,color=color, style="filled", fillcolor=color, fontcolor=fontcolor)
        dot.edge(entity_uid, e_id, style="dashed", arrowhead="none",color=color)
          

dot = Digraph(comment='Query Result')
dot.attr("graph",rankdir="LR",margin="0")

with driver.session() as session:
    session.read_transaction(getEventsDF, dot, "Application", c5_dark_blue, c_white, 3, False)
    session.read_transaction(getEventsDF, dot, "Offer", c5_orange, c_black, 3, False)
    session.read_transaction(getEventsDF, dot, "Workflow", c5_medium_blue, c_black, 3, True)
    session.read_transaction(getDF, dot, "Case_WO", "#999999", c_black, "1")
    session.read_transaction(getDF, dot, "Case_AO", "#777777", c_black, "1")
    session.read_transaction(getDF, dot, "Case_AW", "#555555", c_black, "1")
    session.read_transaction(getResourcesWithinCaseDF, dot)

    session.read_transaction(getEntityForFirstEvent, dot, "Application",c5_dark_blue,c_white)
    session.read_transaction(getEntityForFirstEvent, dot, "Offer",c5_orange,c_black)
    session.read_transaction(getEntityForFirstEvent, dot, "Workflow",c5_medium_blue,c_black)
    session.read_transaction(getEntityForFirstEvent, dot, "Case_R",c5_red,c_white)
    
#print(dot.source)
file = open("bpic17_query_sample-cases.dot","w") 
file.write(dot.source)
file.close()
#dot.render('test-output/round-table.gv', view=True)