/* Regex not currently in use

let edge_label_regex = /([0-9]+) -> ([0-9])+: ([A-Za-z ]+)+/gm;

Was written to do the entire capture in one regex, but this isn't exactly possible, the last node overwrites
the last entry in the capture group

let op_regex = /([0-9]+)<([0-9]+)>\[([A-Za-z]{1})\(([A-Za-z-_]+)\)\]: (([0-9]+)<([0-9]+)>\[([A-Za-z]{1})\(([A-Za-z-_]+)\)\][ ]?)+/gm;
*/


const hOffset = 75;
const vOffset = 150;

let node_width = 75;
let node_height = 50;


// Returns the new adj_list with the implicit edges connection operations of the same transaction in order
function addImplicitEdges(adj_list){

	// First we have to find an ordered list of operations for each transaction

	let ordered_ops = new Map();

	adj_list.forEach((value, key) => {
		let parsed_key = JSON.parse(key);
		// Ensure that the txn -> [...]
		if(!ordered_ops.has(parsed_key.node.label)){
			ordered_ops.set(parsed_key.node.label, []);
		}

		// Pushing the operation onto the list of operations for each transaction
		ordered_ops.get(parsed_key.node.label).push(parsed_key);

	});
	ordered_ops.forEach((value, key) => {
		if(value.length > 0){
			// Sorting the list based on the operation's number
			let txn_ordered = value.sort((a, b) => a.node.op_num - b.node.op_num);
			for(let i = 0; i < txn_ordered.length - 1; i++){
				// Adding the adjacent edges
				adj_list.get(JSON.stringify(txn_ordered[i])).push(txn_ordered[i+1]);
			}
		}

	});

	return adj_list;
}

// Take the node format given by isodiff and translate it to a standard format
function translateNode(isodiffNode){
	let readOrWrite = (isodiffNode.op == 0) ? "R" : "W";
	let newNode = {
		node: {
			op_num: isodiffNode.index,
			txn_id: isodiffNode.txn_index,
			index: isodiffNode.index,
			type: readOrWrite,
			sql: isodiffNode.sql_index,
		}
	}
	return newNode;
}

function genGraph(log){

	// Creating the initial adjacency map
	let adj_list = new Map();

	let OpMap = new Map();
	let TxnMap = new Map();

	let nodes = log.OpDepGraph.OpNodes;
	let txns = log.TxnDepGraph.TxnNodes;

	for(let i = 0; i < txns.length; i++){
		TxnMap.set(txns[i].index, txns[i].tid);
	}
	// Creating a mapping OpMap: index -> operation
	for(let i = 0; i < nodes.length; i++){
		let translatedNode = translateNode(nodes[i]);
		let ids = TxnMap.get(nodes[i].txn_index);
		translatedNode.node.major_id = ids.txn_major;
		translatedNode.node.minor_id = ids.txn_minor;
		translatedNode.node.label = "("+ids.txn_major+","+ids.txn_minor+")";
		OpMap.set(nodes[i].index, JSON.stringify(translatedNode));
	}

	let edges = log.OpDepGraph.Edges;

	for(let i = 0; i < edges.length; i++){
		
		let src = OpMap.get(edges[i].src);
		let dst = OpMap.get(edges[i].dst);
		
		if(!adj_list.has(src)){
			adj_list.set(src, []);
		}

		if(!adj_list.has(dst)){
			adj_list.set(dst, []);
		}
		
		adj_list.get(src).push(JSON.parse(dst));
	}

	adj_list = addImplicitEdges(adj_list);

	return adj_list;
}

// Returns whether or not the node has any incoming edges
function hasInEdges(adj_list, node){

	let map_list_form = [...adj_list.entries()]
	for(let i = 0; i < map_list_form.length; i++){
		let edge_list = map_list_form[i][1]
		for(let j = 0; j < edge_list.length; j++){
			if(JSON.stringify(edge_list[j]) == node){
				return true;
			}
		}	
	}
	
	return false;
}

// Initializes the node in the map, returns the new map
function addNode(adj_list, node){
	adj_list.set(JSON.stringify(node), []);
	return adj_list;
}

// Adds the dest node the src nodes list of destination nodes, returns the new map
function addEdge(adj_list, src, dst){
	adj_list.get(JSON.stringify(src)).push(dst);
	return adj_list;
}

// Removes the node from the adjacency list and returns new list
function removeNode(adj_list, node){
	let res = adj_list.delete(node);
	// Iterates over each element in that map looking for edges that
	// go to the node and removes the destination from the list
	adj_list.forEach((value, key) => {
		for(let i = 0; i < value.length; i++){

			// If the element in the list is equal to the node we want to remove
			// then remove the node
			if(JSON.stringify(value[i]) == node){
				adj_list.set(key, value.splice(i, 1));
			}
		}
	});

	return adj_list;
}

// Returns the key of a node that has an indegree of 0
function findNoIncomingNode(adj_list){
	let keys = [...adj_list.keys()];
	for(let i = 0; i < keys.length; i++){
		if(!hasInEdges(adj_list, keys[i])){
			return keys[i];
		}
	}
}

// Returns an ordered list of nodes
function topoSort(adj_list){
	let ordered_edges = [];
	while(adj_list.size > 0){
		let no_incoming = findNoIncomingNode(adj_list);
		ordered_edges.push(no_incoming);
        let beg_size = adj_list.size;
		adj_list = removeNode(adj_list, no_incoming);
        let end_size = adj_list.size;
        if(beg_size == end_size){
            console.log("topoSort in endless loop")
            break;
        }
	}

	return ordered_edges;
}

// Returns the object to pass to D3 to represent the vertices and edges
function getGraphLayout(adj_list, edge_attrs){

	let adj_list_copy = new Map(adj_list);

	let node_order = topoSort(adj_list_copy);

	console.log("Finished parsing and ordering");

	let g = {
		nodes: [],
		links: []
	};

	let txn_set = new Set();

	adj_list.forEach((value, key) => {
		let parsed_key = JSON.parse(key);
		txn_set.add(parsed_key.node.label);
	});

	let sorted_list = [...txn_set];

	// Sorting the nodes based on their txn id
	sorted_list.sort((a, b) => {
		return a.toString().localeCompare(b);
	});

	let txn_mapping = new Map();

	for(let i = 0; i < sorted_list.length; i++){
		txn_mapping.set(sorted_list[i], i);
	}

	let txn_list = [...txn_mapping.keys()];

	// List of the nodes in graphable form
	for(let i = 0; i < node_order.length; i++){
		
		let curr_raw_order = node_order[i];
		let curr = JSON.parse(curr_raw_order);

		let gnode = {
			"name": curr.node.type + "(" + curr.node.op_num + ")",
			"type": curr.node.type,
			"sql": curr.node.sql,
			"op_num": curr.node.op_num,
			"txn_id": curr.node.txn_id,
			"major_id": curr.node.major_id,
			"minor_id": curr.node.minor_id,
			"index": curr.node.index,
			"label": curr.node.label,
			"x": 50 + (i*hOffset),
			"y": 50 + (txn_mapping.get(curr.node.label)*vOffset)
		};

		g.nodes.push(gnode);

	}

	for(let i = 0; i < node_order.length; i++){

		let curr_raw_order = node_order[i];
		let curr = JSON.parse(curr_raw_order);

		let curr_edges = adj_list.get(JSON.stringify(curr));

		let name = curr.node.type + "(" + curr.node.op_num + ")";

		let x_cord = 50 + (i*hOffset);
		let y_cord = 50 + (txn_mapping.get(curr.node.label)*vOffset);

		console.log("T" + curr.node.label + ": " + name + " @ (" + x_cord + ", " + y_cord + ")");
		let gnode = {
			"name": name,
			"type": curr.node.type,
			"sql": curr.node.sql,
			"op_num": curr.node.op_num,
			"txn_id": curr.node.txn_id,
			"major_id": curr.node.major_id,
			"minor_id": curr.node.minor_id,
			"index": curr.node.index,
			"label": curr.node.label,
			"x": x_cord,
			"y": y_cord
		};

		for(let j = 0; j < curr_edges.length; j++){
			let e = curr_edges[j];

			// Not adding edges between operations in the same transaction, they're implicit
			if(gnode.label == e.node.label){
				continue;
			}

			let link_src = Object.assign({}, gnode);
			let link_dst = Object.assign({}, g.nodes.filter(function(n){
					return e.node.txn_id == n.txn_id && e.node.index == n.index;
				})[0]);

			// Draw the arrow between nodes from the edge to the edge rather than to the center
			if(link_src.y > link_dst.y){
				link_dst.y += node_height;
			} else {
				link_src.y += node_height;
			}

			link_src.x += node_width/2;
			link_dst.x += node_width/2;
			g.links.push({
				"source": link_src,
				"target": link_dst,
				"attrs": edge_attrs.get(link_src.index+","+link_dst.index)
			});
		}

	}

	g.txns = [];

	for(let i = 0; i < txn_list.length; i++){
		g.txns.push({
			txn: txn_list[i],
			x: 10,
			y: 50 + (i*vOffset) + node_height/2
		});
	}

	return g;
}
/*
let testlist = new Map();
let n1 = newTestNode(1);
let n2 = newTestNode(2);

testlist = addNode(testlist, n1);
testlist = addNode(testlist, n2);

testlist = addEdge(testlist, n1, n2);

console.log(findNoIncomingNode(testlist) == n1);

*/

//testlist = removeNode(testlist, n1);

//console.log(findNoIncomingNode(testlist) == n2);

/*
console.log(topoSort(testlist));

let adj_list = genGraph(infile);
console.log(topoSort(adj_list));
*/

/*
let ag = new Map();

ag = addNode(ag, "B");
ag = addNode(ag, "A");
ag = addNode(ag, "C");
ag = addNode(ag, "D");
ag = addNode(ag, "E");

ag = addEdge(ag, "A", "B");
ag = addEdge(ag, "B", "C");
ag = addEdge(ag, "C", "D");
ag = addEdge(ag, "D", "E");

console.log(topoSort(ag));
*/
