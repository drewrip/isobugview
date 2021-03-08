let infile_test = `
-----------------------------
BEGIN Transaction Dependency Graph:
9 -> 13: WR WR WR WR WR WR WR WR WR WR WR 
13 -> 9: vRW vRW vRW vRW vRW vRW 
END Transaction Dependency Graph
Operation Dependency Graph:
565<9>[W(order_line-ol_w_id)]: 650<13>[R(order_line-ol_w_id)]
644<13>[R(order_line-ol_d_id)]: 528<9>[W(order_line-ol_d_id)] 558<9>[W(order_line-ol_d_id)]
556<9>[W(stock-s_quantity)]: 651<13>[R(stock-s_quantity)]
560<9>[W(order_line-ol_i_id)]: 654<13>[R(order_line-ol_i_id)] 647<13>[R(order_line-ol_i_id)]
646<13>[R(order_line-ol_w_id)]: 565<9>[W(order_line-ol_w_id)] 535<9>[W(order_line-ol_w_id)]
562<9>[W(order_line-ol_o_id)]: 649<13>[R(order_line-ol_o_id)]
558<9>[W(order_line-ol_d_id)]: 648<13>[R(order_line-ol_d_id)]
535<9>[W(order_line-ol_w_id)]: 650<13>[R(order_line-ol_w_id)]
530<9>[W(order_line-ol_i_id)]: 647<13>[R(order_line-ol_i_id)] 654<13>[R(order_line-ol_i_id)]
645<13>[R(order_line-ol_o_id)]: 532<9>[W(order_line-ol_o_id)] 562<9>[W(order_line-ol_o_id)]
528<9>[W(order_line-ol_d_id)]: 648<13>[R(order_line-ol_d_id)]
532<9>[W(order_line-ol_o_id)]: 649<13>[R(order_line-ol_o_id)]
END Operation Dependency Graph
SQL <---> Operation Map:
[5,2,43]: 556
[5,2,40]: 532 535 530 528
[7,2,54]: 648 651 650 649 654
[7,2,53]: 645 644 646 647
[5,2,44]: 558 560 565 562
End SQL-Op Map.
-----------------------------
`;

let infile_short_test = `
BEGIN Transaction Dependency Graph:
8 -> 4: vRW 
4 -> 8: vRW WR WW vRW 
END Transaction Dependency Graph
Operation Dependency Graph:
247<4>[R(district-d_next_o_id)]: 411<8>[W(district-d_next_o_id)]
469<8>[R(stock-s_quantity)]: 282<4>[W(stock-s_quantity)]
279<4>[R(stock-s_quantity)]: 472<8>[W(stock-s_quantity)]
251<4>[W(district-d_next_o_id)]: 407<8>[R(district-d_next_o_id)] 411<8>[W(district-d_next_o_id)]
END Operation Dependency Graph
SQL <---> Operation Map:
[3,1,20]: 247
[3,1,26]: 282
[5,1,43]: 472
[5,1,42]: 469
[3,1,25]: 279
[3,1,21]: 251
[5,1,33]: 407
[5,1,34]: 411
End SQL-Op Map.

`

let infile_basic_test = `
-----------------------------
BEGIN Transaction Dependency Graph:
(4,2) -> (6,1): vRW 
(6,1) -> (4,2): WR 
END Transaction Dependency Graph
Operation Dependency Graph:
379<[4,2,29]>[R(customer-c_balance)]: 594<[6,1,51]>[W(customer-c_balance)]
582<[6,1,48]>[W(orders-o_carrier_id)]: 387<[4,2,30]>[R(orders-o_carrier_id)]
END Operation Dependency Graph
SQL-Op Map:
[6,1,51]: 594
[4,2,29]: 379
[6,1,48]: 582
[4,2,30]: 387
End SQL-Op Map.
-----------------------------
`

exports.default_test = `
-----------------------------
BEGIN Transaction Dependency Graph:
7 -> 10: vRW 
7 -> 4: vRW 
10 -> 4: vRW vRW vRW vRW vRW 
4 -> 7: WR WR WR WR 
END Transaction Dependency Graph
Operation Dependency Graph:
581<10>[R(orders-o_w_id)]: 258<4>[W(orders-o_w_id)]
578<10>[R(orders-o_c_id)]: 253<4>[W(orders-o_c_id)]
255<4>[W(orders-o_entry_d)]: 388<7>[R(orders-o_entry_d)]
387<7>[R(orders-o_carrier_id)]: 582<10>[W(orders-o_carrier_id)]
576<10>[R(orders-o_id)]: 256<4>[W(orders-o_id)]
575<10>[R(orders-o_d_id)]: 254<4>[W(orders-o_d_id)]
256<4>[W(orders-o_id)]: 389<7>[R(orders-o_id)] 386<7>[R(orders-o_id)]
577<10>[R(orders-o_w_id)]: 258<4>[W(orders-o_w_id)]
258<4>[W(orders-o_w_id)]: 385<7>[R(orders-o_w_id)]
384<7>[R(orders-o_d_id)]: 254<4>[W(orders-o_d_id)]
END Operation Dependency Graph
SQL <---> Operation Map:
[6,1,48]: 581 582
[4,2,30]: 388 389 384 385 387 386
[6,1,47]: 578 575 577 576
[3,1,22]: 255 254 253 258 256
End SQL-Op Map.
-----------------------------
`

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
		if(!ordered_ops.has(parsed_key.node.txn_id)){
			ordered_ops.set(parsed_key.node.txn_id, []);
		}

		// Pushing the operation onto the list of operations for each transaction
		ordered_ops.get(parsed_key.node.txn_id).push(parsed_key);

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

exports.genGraph = function(log){
	// Identifies all lines in the log that declare the edges
	let edge_regex = /[0-9]+<[0-9]+>\[[A-Za-z]{1}\([A-Za-z-_]+\)\]: (?:[0-9]+<[0-9]+>\[[A-Za-z]{1}\([A-Za-z-_]+\)\][ ]?)+/gm;

	// Identifies the nodes that are used in the declaration of an edge
	let node_regex = /([0-9]+)<([0-9]+)>\[([A-Za-z]{1})\(([A-Za-z-_]+)\)\]/gm;


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
				type: parsed_ops[0][3],
				sql: parsed_ops[0][4],
			}
		};

		// Iterating over all of the nodes in the line
		for(let j = 1; j < parsed_ops.length; j++){
			let dst = {
				node: {
					op_num: parsed_ops[j][1],
					txn_id: parsed_ops[j][2],
					type: parsed_ops[j][3],
					sql: parsed_ops[j][4],
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
	let res = false;
	adj_list.forEach(value => {
		value.forEach(n => {
			if(JSON.stringify(n) == node){
				res = true;
			}
		});
	});
	return res;
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

	console.log("Has: " + adj_list.has(node));
	// Deletes the node from the map
	console.log("Deleted: " + adj_list.delete(node));

	// Iterates over each element in that map looking for edges that
	// go to the node and removes the destination from the list
	adj_list.forEach((value, key) => {
		for(let i = 0; i < value.length; i++){

			// If the element in the list is equal to the node we want to remove
			// then remove the node

			console.log(JSON.stringify(value[i]) == node);
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
		console.log(no_incoming);
		ordered_edges.push(no_incoming);
		adj_list = removeNode(adj_list, no_incoming);
	}

	return ordered_edges;
}

// Returns the object to pass to D3 to represent the vertices and edges
exports.getGraphLayout = function(adj_list){

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
		txn_set.add(parsed_key.node.txn_id);
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
			"x": 50 + (i*hOffset),
			"y": 50 + (txn_mapping.get(curr.node.txn_id)*vOffset)
		};

		g.nodes.push(gnode);

	}

	for(let i = 0; i < node_order.length; i++){

		let curr_raw_order = node_order[i];
		let curr = JSON.parse(curr_raw_order);

		let curr_edges = adj_list.get(JSON.stringify(curr));

		let name = curr.node.type + "(" + curr.node.op_num + ")";

		let x_cord = 50 + (i*hOffset);
		let y_cord = 50 + (txn_mapping.get(curr.node.txn_id)*vOffset);

		console.log("T" + curr.node.txn_id + ": " + name + " @ (" + x_cord + ", " + y_cord + ")");
		let gnode = {
			"name": name,
			"type": curr.node.type,
			"sql": curr.node.sql,
			"op_num": curr.node.op_num,
			"txn_id": curr.node.txn_id,
			"x": x_cord,
			"y": y_cord
		};

		for(let j = 0; j < curr_edges.length; j++){
			let e = curr_edges[j];

			// Not adding edges between operations in the same transaction, they're implicit
			if(gnode.txn_id == e.node.txn_id){
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
