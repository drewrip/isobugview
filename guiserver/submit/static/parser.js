/* Regex not currently in use

let edge_label_regex = /([0-9]+) -> ([0-9])+: ([A-Za-z ]+)+/gm;

Was written to do the entire capture in one regex, but this isn't exactly possible, the last node overwrites
the last entry in the capture group

let op_regex = /([0-9]+)<([0-9]+)>\[([A-Za-z]{1})\(([A-Za-z-_]+)\)\]: (([0-9]+)<([0-9]+)>\[([A-Za-z]{1})\(([A-Za-z-_]+)\)\][ ]?)+/gm;
*/


const hOffset = 75;
const vOffset = 150;

const log_sep = "-----------------------------";
let node_width = 75;
let node_height = 50;


// The output log will have multiple graphs contained inside, so we need to split these up so we
// can parse each of them individually
function splitLog(raw_log){
	let log_list = [];

	let lines = raw_log.split("\n");

	let i = 0;
	let numLines = lines.length;

	// Throw out all of the lines before the first log
	while(i < numLines){
		if(lines[i] == log_sep){
			break;
		} else {
			i++;
		}
	}

	let finished_log_ind = 0;
	let curr_log = "";
	let capture = false;
	while(i < numLines){
		if((lines[i] == log_sep) && !capture){
			i++;
			capture = true;
		} else if((lines[i] == log_sep) && capture){
			log_list[finished_log_ind] = curr_log;
			curr_log = "";
			finished_log_ind++;
			i++;
			capture = false;
		} else if(lines[i] != log_sep && capture){
			curr_log += lines[i] + "\n";
			i++;
		} else {
			console.log(lines[i]);
			i++;
		}
	}
	
	return log_list;
}

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

function genGraph(log){

	// Identifies all lines in the log that declare the edges
	let edge_regex = /[0-9]+<\[[0-9]+,[0-9]+,[0-9]+\]>\[[A-Za-z]{1}\([A-Za-z-_\[\]]+\)\]: (?:[0-9]+<\[[0-9]+,[0-9]+,[0-9]+\]>\[[A-Za-z]{1}\([A-Za-z-_\[\]]+\)\][ ]?)+/gm;

	// Identifies the nodes that are used in the declaration of an edge
	let node_regex = /([0-9]+)<\[([0-9]+),([0-9]+),[0-9]+\]>\[([A-Za-z]{1})\(([A-Za-z-_\[\]]+)\)\]/gm;


	// Matching all of the edges in the file
	const edges = log.match(edge_regex);

	// Creating the initial adjacency map
	let adj_list = new Map();

	// Iterating over each of the matched lines in the regex
	for(let i = 0; i < edges.length; i++){

		// Getting the array of matched elements
		let parsed_edge = edges[i];

		let parsed_ops = [...parsed_edge.matchAll(node_regex)];

		// Creating a source node object from the first 4 matched elements
		let src = {
			node: {
				op_num: parsed_ops[0][1],
				txn_id: parsed_ops[0][2],
				minor_id: parsed_ops[0][3],
				label: "("+parsed_ops[0][2]+","+parsed_ops[0][3]+")",
				type: parsed_ops[0][4],
				sql: parsed_ops[0][5],
			}
		};

		// Iterating over all of the nodes in the line
		for(let j = 1; j < parsed_ops.length; j++){
			let dst = {
				node: {
					op_num: parsed_ops[j][1],
					txn_id: parsed_ops[j][2],
					minor_id: parsed_ops[j][3],
					label: "("+parsed_ops[j][2]+","+parsed_ops[j][3]+")",
					type: parsed_ops[j][4],
					sql: parsed_ops[j][5],
				}
			};

			// If the source node doesn't exist in the adjacency list, set it equal to a list first
			if(!adj_list.has(JSON.stringify(src))){
				adj_list.set(JSON.stringify(src), []);
			}

			// Push the destination node to the list of the source node
			adj_list.get(JSON.stringify(src)).push(dst);

			// Adding the dst node to the list of key nodes if it doesn't exist
			if(!adj_list.has(JSON.stringify(dst))){
				adj_list.set(JSON.stringify(dst), []);
			}

		}

	}

	adj_list = addImplicitEdges(adj_list);

	return adj_list;
}

function newTestNode(n){
	let test = {
		node: {
			op_num: n,
			txn_id: n,
			type: n,
			sql: n,
		}
	};
	return test;
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
function getGraphLayout(adj_list){

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
			"minor_id": curr.node.minor_id,
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
					return e.node.txn_id == n.txn_id && e.node.op_num == n.op_num;
				})[0]);

			// Draw the arrow between nodes from the edge to the edge rather than to the center
			if(link_src.y > link_dst.y){
				link_src.y -= node_height/2;
				link_dst.y += node_height/2;
			} else {
				link_src.y += node_height/2;
				link_dst.y -= node_height/2;
			}
			g.links.push({
				"source": link_src,
				"target": link_dst
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
