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


def getNodeLabel_Event(name):
    return name[2:7]

def getDFcNodes(tx, dot, entity_prefix, entity_name, clusternumber, color, fontcolor, min_freq):
    q = f'''
        MATCH (c1:Class {{Type:"Activity+Lifecycle"}}) -[df:DF_C {{SubQuery:"1"}}]- ()
        WHERE df.count > {min_freq}
        return distinct c1
        '''
    print(q)

    dot.attr("node",shape="circle",fixedsize="true", width="0.4", height="0.4", fontname="Helvetica", fontsize="8", margin="0")    
    c_entity = Digraph(name="cluster"+str(clusternumber))
    c_entity.attr(rankdir="LR", style="invis")
    
    for record in tx.run(q):
        c1_id = str(record["c1"].id)
        if record["c1"]["Name"][0:2] == entity_prefix:
            c1_name = getNodeLabel_Event(record["c1"]["Name"])+'\\n'+record["c1"]["Lifecycle"][0:5]
            c_entity.node(c1_id,c1_name, color=color, style="filled", fillcolor=color, fontcolor=fontcolor)
            
    q = f'''
        MATCH (c1:Class {{Type:"Activity+Lifecycle"}})
        WHERE NOT (:Class)-[:DF_C {{ EntityType: "{entity_name}", SubQuery:"1"}}]->(c1)
        return distinct c1
        '''
    print(q)
    for record in tx.run(q):
        c1_id = str(record["c1"].id)
        if record["c1"]["Name"][0:2] == entity_prefix:
            c1_id = str(record["c1"].id)
            dot.node(entity_name, entity_name,shape="rectangle",fixedsize="false",color=color, style="filled", fillcolor=color, fontcolor=fontcolor)
            dot.edge(entity_name, c1_id, style="dashed", arrowhead="none",color=color)
    
    
    dot.subgraph(c_entity)


def getDFcEdges(tx, dot, entity, edge_color, minlen, edge_label, show_count, min_freq):
    q = f'''
        MATCH (c1:Class {{Type:"Activity+Lifecycle"}}) -[df:DF_C {{SubQuery:"1"}}]-> (c2:Class {{Type:"Activity+Lifecycle"}})
        WHERE df.count > {min_freq} and df.EntityType = "{entity}"
        return distinct c1,df,c2
        '''
    print(q)

    dot.attr("node",shape="circle",fixedsize="true", width="0.4", height="0.4", fontname="Helvetica", fontsize="8", margin="0")
    dot.attr("edge",fontname="Helvetica", fontsize="8")
    for record in tx.run(q):
        c1_id = str(record["c1"].id)
        c2_id = str(record["c2"].id)
        
        #c1_name = getNodeLabel_Event(record["c1"]["Name"])+'\n'+record["c1"]["Lifecycle"][0:5]
        #c2_name = getNodeLabel_Event(record["c2"]["Name"])+'\n'+record["c2"]["Lifecycle"][0:5]
        
        xlabel = str(record["df"]["count"]) #edge_label+" ("+str(record["df"]["count"])+")"
        penwidth = 1 + record["df"]["count"] / 50000
        
        #dot.node(c1_id,c1_name)
        #dot.node(c2_id,c2_name)
        if record["df"]["count"] < 1000:
            constraint = "false"
        else:
            constraint = "true"
        dot.edge(c1_id,c2_id, xlabel=xlabel,fontcolor=edge_color,color=edge_color,penwidth=str(penwidth),constraint=constraint)

dot = Digraph(comment='Query Result')
dot.attr("graph",rankdir="LR",margin="0", compound="true")

with driver.session() as session:
    session.read_transaction(getDFcNodes, dot, "A_", "Application", 0, c5_dark_blue, c_white, 500)
    session.read_transaction(getDFcNodes, dot, "W_", "Workflow", 1, c5_medium_blue, c_black, 500)
    session.read_transaction(getDFcNodes, dot, "O_", "Offer", 2, c5_orange, c_black, 500)
    session.read_transaction(getDFcEdges, dot, "Application", c5_dark_blue, 2, "", True, 500)
    session.read_transaction(getDFcEdges, dot, "Workflow", c5_medium_blue, 1, "", True, 500)
    session.read_transaction(getDFcEdges, dot, "Offer", c5_orange, 2, "", True, 500)
    session.read_transaction(getDFcEdges, dot, "Case_AO", "#777777", 0, "AO", True, 500)
    session.read_transaction(getDFcEdges, dot, "Case_WO", "#999999", 0, "WO", True, 500)
    session.read_transaction(getDFcEdges, dot, "Case_AW", "#555555", 0, "AW", True, 500)

print(dot.source)
file = open("bpic17_query_aggregated-df-multi-entity.dot","w") 
file.write(dot.source)
file.close()
#dot.render('test-output/round-table.gv', view=True)