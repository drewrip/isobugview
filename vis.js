const D3Node = require("d3-node");
const parser = require("./parser");

const hOffset = 75;
const vOffset = 150;

let node_width = 75;
let node_height = 50;

function visualize(){
    let log = document.getElementById("log_input").value;

    // Remove any existing graph

    let curr_graphs = document.getElementsByTagName("svg");
    for(let i = 0; i < curr_graphs.length; i++){
        curr_graphs[i].remove();
    }
    

    render_graph(log);
}

exports.drawGraph = function(log){

    let adj_list = parser.genGraph(log);
    let g = parser.getGraphLayout(adj_list);

	var d3n = new D3Node("#graph_area");

	const d3 = d3n.d3;

	var svg = d3n.createSVG((node_width * g.nodes.length) + 150, 400).lower()
	
    svg.append("defs").append("marker")
        .attr("id", "arrow")
        .attr("viewBox", "0 -5 10 10")
        .attr("refX", 10)
        .attr("refY", 0)
        .attr("markerWidth", 5)
        .attr("markerHeight", 10)
        .attr("orient", "auto")
        .append("svg:path")
        .attr("color", "black")
        .attr("d", "M0,-5L10,0L0,5");

    let transactions = svg.selectAll(".txn_label")
        .data(g.txns)
        .enter()
        .append("text")
        .text(function(d){
            return d.txn;
        })
        .attr("class", "txn_label")
        .attr("x", function(d){
            return d.x;
        })
        .attr("y", function(d){
            return d.y;
        });

    var link = svg.selectAll("line")
        .data(g.links)
        .enter()
        .append("line")
        .attr("class", "links")
        .attr("x1", function(d){
            return d.source.x + (node_width/2);
        })
        .attr("y1", function(d){
            return d.source.y + (node_height/2);
        })
        .attr("x2", function(d){
            return d.target.x + (node_width/2);
        })
        .attr("y2", function(d){
            return d.target.y + (node_height/2);
        })
        .attr("stroke-width", 3)
        .attr("stroke", "black")
        .attr("marker-end", "url(#arrow)")
        .on("mouseover", function(d, i){
            d3.select("#el"+i).attr("visibility", "visible");
            d3.select(this).attr("stroke", "orange");

            d3.select("p").attr("visibility", "visible");

            document.getElementById("sql_area").style.visibility = "visible";
            document.getElementById("op_type").innerHTML = d.source.type + d.target.type;
            document.getElementById("sql_pair").innerHTML = d.source.sql + "🠖" + d.target.sql;
        })
        .on("mouseout", function(d, i){
            d3.select("#el"+i).attr("visibility", "hidden");
            d3.select(this).attr("stroke", "black");

            document.getElementById("sql_area").style.visibility = "hidden";
            document.getElementById("op_type").innerHTML = "";
            document.getElementById("sql_pair").innerHTML = "";
        });

    /*
      var edge_labels = svg.selectAll(".labels")
      .data(g.links)
      .enter()
      .append("text")
      .text(function(d) {
      return d.source.type + d.target.type;
      })
      .attr('x', function(d) {
      return (d.source.x + d.target.x)/2;
      })
      .attr('y', function(d) {
      return (d.source.y + d.target.y)/2;
      })
      .attr("id", function(d, i){
      return "el" + i
      })
      .attr("visibility", "hidden");

    */
    
    // Adding the nodes
    var node = svg.append("g")
        .attr("class", "nodes")
        .selectAll("g")
        .data(g.nodes)
        .enter().append("g");

    var rect = node.append("rect")
        .attr("x", function(d){
            return d.x;
        })
        .attr("y", function(d){
            return d.y;
        })
        .attr("width", node_width)
        .attr("height", node_height)
        .attr("rx", 5)
        .attr("ry", 5)
        .attr("stroke", "#000000")
        .attr("stroke-width", 2)
        .attr("fill", "#FFFFFF")
        .attr("fill-width", 5);

    var node_labels = node.append("text")
        .text(function(d) {
			return d.type + "(" + d.op_num + ")";
        })
        .attr('x', function(d){
            return d.x + 10;
        })
        .attr('y', function(d){
            return d.y + 30;
        });

    node.append("title")
        .text(function(d) { return d.id; });

    svg.append("type").attr("class", "labels");


    var node = svg.append("g")
        .attr("class", "nodes")
        .selectAll("g")
        .data(g.nodes)
        .enter().append("g");

	return d3n
}
