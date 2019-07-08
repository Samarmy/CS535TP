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

  chart(g, circles)

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

function colorByOutDegree(d, nodes, color){
	if (nodes.has(d.id)){
		return d3.rgb(color)
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

function chart(data, circles) {
	var out_counts = {}
	for(l in data.links){
		if (data.links[l].source in out_counts){
			out_counts[data.links[l].source] += 1
		} else {
			out_counts[data.links[l].source] = 1
		}
		if (data.links[l].target in out_counts){
			out_counts[data.links[l].target] += 1
		} else {
			out_counts[data.links[l].target] = 1
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

	var pie_width = 200
	var pie_height = 200

	pie_color = d3.scaleOrdinal()
		.domain(pie_data.map(d => d.name))
		.range(d3.quantize(t => d3.interpolateSpectral(t * 0.8 + 0.1), pie_data.length).reverse())
	
	arc = d3.arc()
		.innerRadius(0)
		.outerRadius(Math.min(pie_width, pie_height) / 2 - 1)
		
	function arcLabel() {
	  const pie_radius = Math.min(pie_width, pie_height) / 2 * 0.8;
	  return d3.arc().innerRadius(pie_radius).outerRadius(pie_radius);
	}

	pie = d3.pie()
		.sort(null)
		.value(d => d.value)
		
  const arcs = pie(pie_data)

  const svg = d3.select("svg")
    
  const g = svg.append("g")
      .attr("transform", `translate(${width/2},${height*5/6})`)
      
  g.append("text")
   .text("Degree Distribution")
   .attr("x", "0.0em")
   .attr("y", "-6.0em")
   .style("font-weight", "bold")
   .style("font-size", "20px")
   .style("text-anchor", "middle");
  
  g.selectAll("path")
    .data(arcs)
    .enter().append("path")
      .attr("fill", d => pie_color(d.data.name))
      .attr("stroke", "white")
      .attr("d", arc)
     .on("mouseover", function (d) {
		d3.select(this).transition()
          .duration(500)
          .attr("d", d3.arc()
			.innerRadius(0)
			.outerRadius(Math.min(pie_width, pie_height) / 2 - 1 + 20));
		var newColor = d3.select(this).attr('fill')
		nodes = new Set(degree_map[parseInt(d.data.name)].map(Number))
		circles.attr("fill", function(d2) { return colorByOutDegree(d2, nodes, newColor) })
	})
	.on("mouseout", function(d) {
        d3.select(this).transition()
          .duration(500)
          .attr("d", d3.arc()
			.innerRadius(0)
			.outerRadius(Math.min(pie_width, pie_height) / 2 - 1));	
		circles.attr("fill", function(d2) { return colorByOutDegree(d2, new Set(), d3.rgb("black")) })
      })
    .append("title")
      .text(d => `${d.data.name}: ${d.data.value.toLocaleString()}`);

  const text = g.selectAll("text")
    .data(arcs)
    .enter().append("text")
      .attr("transform", d => `translate(${arcLabel().centroid(d)})`)
      .attr("dy", "0.35em")
  
  text.append("tspan")
      .attr("x", "-0.3em")
      .attr("y", "0.0em")
      .style("font-weight", "bold")
      .text(d => d.data.name);
  /*
  text.filter(d => (d.endAngle - d.startAngle) > 0.25).append("tspan")
      .attr("x", 0)
      .attr("y", "0.7em")
      .attr("fill-opacity", 0.7)
      .text(d => d.data.value.toLocaleString());
	*/
  return svg.node();
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
