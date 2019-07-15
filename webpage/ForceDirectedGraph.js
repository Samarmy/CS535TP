var hashtagList = {"win": 1, "politics":2, "tech":3, "science":4, "summer":5, "funny":6,
					"happybirthday":7, "metoo":8,"photography":9,"marvel":10,"pets":11, "friends":12,
					"birthday":13,"technology":14, "fashion":15, "trump":16, "impeachdonaldtrump":26, "news":18, "fakenews":19, "family":20, "food":21,
					"usa":22, "love":23, "men":24, "women":25}

var svg = d3.select("svg"),
    width = +svg.attr("width"),
    height = +svg.attr("height");

var color = d3.scaleOrdinal(d3.schemeCategory10);
var levelSize = 500
var numLevels = 100
var stack = []
var radius = 6
	
fetchData()

var graph = null

function updateSim(error, g, info) {
  if (error) throw error;
  d3.select("svg").selectAll("*").remove()
  var simulation = d3.forceSimulation()
    .force("link", d3.forceLink().id(function(d) { return d.id; }))
    .force("charge", d3.forceManyBody().strength(-150))
    .force("x", d3.forceX(width / 2))
    .force("y", d3.forceY(height*4/6 / 2));
    
  console.log(g)
  graph = g
  d3.selectAll(".links").remove()
  d3.selectAll(".nodes").remove()
  
  var link = svg.append("g")
      .attr("class", "links")
    .selectAll("line")
    .data(graph.links)
    .enter().append("line")
      .attr("stroke-width", function(d) { return Math.sqrt(1); });

  var node = svg.append("g")
      .attr("class", "nodes")
    .selectAll("g")
    .data(graph.nodes)
    .enter().append("g")
    .on("click", function(d) { viewSuperNode(d) })
 
  //var h = document.getElementById("hashtagSelector").value
  var h = null
  var circles = node.append("circle")
      .attr("r", 7)
      //.attr("fill", function(d) { return colorNode(d,h,graph) })
      .call(d3.drag()
          .on("start", dragstarted)
          .on("drag", dragged)
          .on("end", dragended));

  node.append("title")
      //.text(function(d) { return makeTooltip(d,h,graph) })
      .text(function(d) { return makeTooltipReduced(d,h,graph,info) })
  simulation
      .nodes(graph.nodes)
      .on("tick", ticked);

  document.getElementById("graph").addEventListener('highlight_degree', function (e) {
	  addInteriorNodeChart(info, circles, e.detail)
	  addInteriorEdgeChart(info, circles, e.detail)
  });
  document.getElementById("graph").addEventListener('highlight_interior_node', function (e) {
	   addDegreeChart(g, circles, e.detail)
	   addInteriorEdgeChart(info, circles, e.detail)
  });
  document.getElementById("graph").addEventListener('highlight_interior_edge', function (e) {
	   addDegreeChart(g, circles, e.detail)
	   addInteriorNodeChart(info, circles, e.detail)
  });
  addDegreeChart(g, circles, new Set())
  addInteriorNodeChart(info, circles, new Set())
  addInteriorEdgeChart(info, circles, new Set())

  simulation.force("link")
      .links(graph.links);

  function ticked() {
	node
		.attr("cx", function(d) { return d.x = Math.max(radius, Math.min(width - radius, d.x)); })
        .attr("cy", function(d) { return d.y = Math.max(radius, Math.min(height*4/6 - radius, d.y)); });
        
    link
        .attr("x1", function(d) { return d.source.x; })
        .attr("y1", function(d) { return d.source.y; })
        .attr("x2", function(d) { return d.target.x; })
        .attr("y2", function(d) { return d.target.y; });

    node
        .attr("transform", function(d) {
          return "translate(" + d.x + "," + d.y + ")";
        })
  }
  
  function dragstarted(d) {
	  if (!d3.event.active) simulation.alphaTarget(0.3).restart();
	  d.fx = d.x;
	  d.fy = d.y;
	}

	function dragged(d) {
	  d.fx = d3.event.x;
	  d.fy = d3.event.y;
	}

	function dragended(d) {
	  if (!d3.event.active) simulation.alphaTarget(0);
	  d.fx = null;
	  d.fy = null;
	}
}

function colorNode(d, h, g) {
	var predictions = document.getElementById('predictionBox').checked
	if (predictions){
		var pred = g["predictions"][h][d.name]
		if (pred >= 0.1){
			var colorLighter = d3.scaleLinear().domain([0,5])
				.range([d3.rgb(color(hashtagList[h])).darker(), 
							d3.rgb(color(hashtagList[h])).brighter()]);
			return colorLighter(pred); 
		} else { return d3.rgb("black") }
	} else if (h in d.hashtags){
		var targetTime = 0
		var maxTime = new Date(document.getElementById('dateSelector').value).getTime()
		var times = d.hashtags[h]
		times.sort(function(a, b){return a - b})
		for (var time in times){
			if (times[time] <= maxTime){
				targetTime = times[time]
				break
			}
		}
		var difference = maxTime - targetTime
		var hoursDifference = Math.floor(difference/1000/60/60/24);
		if (hoursDifference <= 20){
			var colorLighter = d3.scaleLinear().domain([1,20])
				.range([d3.rgb(color(hashtagList[h])).brighter(), 
							d3.rgb(color(hashtagList[h])).darker()]);
			return colorLighter(hoursDifference); 
		} else { return d3.rgb("black") }
	} else { return }
}

function colorByOutDegree(d, nodes, oldNodes, color, t){
	if (nodes.has(d.id)){
		return d3.rgb(color)
	} else if (oldNodes.has(d.id)) {
		return d3.rgb(d3.select(t).attr('fill'))
	} else {
		return d3.rgb("black")
	}
}

function colorByInteriorNodes(d, nodes, oldNodes, color, t){
	if (nodes.has(d.id)){
		return d3.rgb(color)
	} else if (oldNodes.has(d.id)) {
		return d3.rgb(d3.select(t).attr('fill'))
	} else {
		return d3.rgb("black")
	}
}

function makeTooltip(d, h, g) {
	var predictions = document.getElementById('predictionBox').checked
	if (predictions){
		return d.name+" is predicted to tweet #"+h+" "+Math.ceil(g["predictions"][h][d.name])+" times tomorrow";
	} else if (h in d.hashtags){
		var targetTime = 0
		var maxTime = new Date(document.getElementById('dateSelector').value).getTime()
		var times = d.hashtags[h]
		times.sort(function(a, b){return a - b})
		for (var time in times){
			if (times[time] <= maxTime){
				targetTime = times[time]
				break
			}
		}
		var diff = (maxTime - targetTime)/1000/60/60/24
		return d.name+" last tweeted #"+h+" "+parseFloat(diff).toFixed(2)+" days ago";
	} else {
		return d.name+" has not tweeted #"+h+" in the last 30 days";
	}
}

function viewSuperNode(d){
	stack.push(d.id)
	fetchData()
	currSN.innerText = "Current Super Node: "+d.id
}

function makeTooltipReduced(d, h, g, i) {
	if (i[d.id].numNodes == 1){
		return "Node "+d.id
	} else {
		return "Super node "+d.id+" contains "+i[d.id].numNodes+" interior nodes and "+i[d.id].numEdges+" interior edges"
	}
}

function preprocessData(data){
	var realIDMap = {}
	var addedIDs = new Set()
	var removalNodes = []
	var nodeInfo = {}
	for (d in data.nodes){
		realIDMap[data.nodes[d].realID] = data.nodes[d].id
		
		if (data.nodes[d].id in nodeInfo){
			nodeInfo[data.nodes[d].id].numNodes += 1
		} else {
			nodeInfo[data.nodes[d].id] = {numNodes: 1, numEdges: 0}
		}
		
		if(addedIDs.has(data.nodes[d].id)){
			removalNodes.push(d)
		} else {
			addedIDs.add(data.nodes[d].id)
		}
	}
	for (var i = removalNodes.length - 1; i >= 0; --i) {
		data.nodes.splice(removalNodes[i],1)
	}

	var links = []
	var existingLinks = new Set()
	var linkIxs = {}
	for (d in data.links){
		if (!(data.links[d].source in realIDMap) || !(data.links[d].target in realIDMap))
			continue
		var link = {}
		link.source = realIDMap[data.links[d].source]
		link.target = realIDMap[data.links[d].target]
		link.value = 1
		if(link.source === -1 || link.target === -1){
			nodeInfo[link.source].numEdges += 1
			continue
		}
		if (link.source === link.target){
			nodeInfo[link.source].numEdges += 1
		} else {
			var linkID =""+link.source+","+link.target
			if (!existingLinks.has(linkID)){
				links.push(link)
				existingLinks.add(linkID)
				linkIxs[linkID] = links.length-1
			} else {
				links[linkIxs[linkID]].value += 1
			}
		}
	}
	return [{links: links, nodes: data.nodes}, nodeInfo]
}

function fetchData() {
	var _xhr = new XMLHttpRequest();
    _xhr.addEventListener("load", (function(xhr) {
		var response = xhr.currentTarget.response;
        var queryData = JSON.parse(response);
        queryData = preprocessData(queryData)
        updateSim(null, queryData[0], queryData[1])
    }).bind(this));
    _xhr.open("POST", "http://salt-lake-city.cs.colostate.edu:11777/synopsis", true);
	_xhr.setRequestHeader("Content-Type", "application/x-www-form-urlencoded; charset=UTF-8");
    _xhr.send(stack.join(","));	
}

function updateGraph() {
	d3.selectAll("circle").remove()
	var node = d3.selectAll(".nodes").selectAll("g")
	//node.selectAll("title").remove()
	var h = document.getElementById("hashtagSelector").value
	var circles = node.append("circle")
      .attr("r", 5)
      //.attr("fill", function(d) { return colorNode(d,h,graph) })
      .call(d3.drag()
          .on("start", dragstarted)
          .on("drag", dragged)
          .on("end", dragended));
	node.selectAll("title")
      .text(function(d) { return makeTooltip(d,h,graph) })
}

function addDegreeChart(data, circles, filter) {
	var out_counts = {}
	for(l in data.links){
		var src = null
		var trgt = null
		if (typeof data.links[l].source === "object"){
			src = data.links[l].source.id
			trgt = data.links[l].target.id
		} else {
			src = data.links[l].source
			trgt = data.links[l].target
		}
		if(filter.size != 0 && !filter.has(parseInt(src)) && !filter.has(parseInt(trgt))){
			continue
		}
		if (src in out_counts){
			out_counts[src] += 1
		} else {
			out_counts[src] = 1
		}
		if (trgt in out_counts){
			out_counts[trgt] += 1
		} else {
			out_counts[trgt] = 1
		}
	}
	var out_degrees = {}
	var degree_map = {}
	for(c in out_counts){
		if (out_counts[c] in out_degrees){
			out_degrees[out_counts[c]] += 1
			degree_map[out_counts[c]].push(c)
		} else {
			out_degrees[out_counts[c]] = 1
			degree_map[out_counts[c]] = [c]
		}
	}
	var pie_data = [{name:0,value:0}]
	for (d in out_degrees){
		pie_data.push({name:d, value:out_degrees[d]})
	}
	chart(pie_data, degree_map, circles, "degree")
}

function addInteriorNodeChart(data, circles, filter){
	var pie_data = [{name:0,value:0}]
	var pie_counts = {}
	var intNodeMap = {}
	for (var key in data) {
		if (data.hasOwnProperty(key) && (filter.size == 0 || filter.has(parseInt(key)))) {        
			if (data[key].numNodes in intNodeMap){
				intNodeMap[data[key].numNodes].push(key)
				pie_counts[data[key].numNodes] += 1
			} else {
				intNodeMap[data[key].numNodes] = [key]
				pie_counts[data[key].numNodes] = 1
			}
		}
	}
	for (var key in pie_counts) {
		if (pie_counts.hasOwnProperty(key)) {
			pie_data.push({name:key, value:pie_counts[key]})
		}
	}
	chart(pie_data, intNodeMap, circles, "interior_node")
}

function addInteriorEdgeChart(data, circles, filter){
	var pie_data = [{name:0,value:0}]
	var pie_counts = {}
	var intEdgeMap = {}
	for (var key in data) {
		if (data.hasOwnProperty(key) && (filter.size == 0 || filter.has(parseInt(key)))) {        
			if (data[key].numEdges in intEdgeMap){
				intEdgeMap[data[key].numEdges].push(key)
				pie_counts[data[key].numEdges] += 1
			} else {
				intEdgeMap[data[key].numEdges] = [key]
				pie_counts[data[key].numEdges] = 1
			}
		}
	}
	for (var key in pie_counts) {
		if (pie_counts.hasOwnProperty(key)) {
			pie_data.push({name:key, value:pie_counts[key]})
		}
	}
	chart(pie_data, intEdgeMap, circles, "interior_edge")
}

document.getElementById("refreshButton").onclick = function() {
	stack = []
	fetchData()
	currSN.innerText = "Current Super Node: None"
}

document.getElementById("popButton").onclick = function() {
	stack.pop()
	fetchData()
	currSN.innerText = "Current Super Node: "+stack[stack.length-1]
}
/*
document.getElementById("hashtagSelector").onchange = function() {
	updateGraph()
}

document.getElementById("refreshButton").onclick = function() {
	fetchData()
}

document.getElementById('dateSelector').value = new Date().toISOString().substring(0, 10);

document.getElementById('dateSelector').onchange = function(){
	updateGraph()
}

document.getElementById('predictionBox').onchange = function(){
	updateGraph()
}
*/
