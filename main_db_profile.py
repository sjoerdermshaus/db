import main_db


from pycallgraph import PyCallGraph
from pycallgraph.output import GraphvizOutput

graphviz = GraphvizOutput()
graphviz.output_file = 'basic.png'
with PyCallGraph(output=graphviz):
    main_db.main()
