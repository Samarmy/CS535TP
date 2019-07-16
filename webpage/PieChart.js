function chart(pie_data, data_map, circles, innerCircles, selected, type) {
	d3.select("g."+type).remove();
	var pie_width = 200
	var pie_height = 200
	var selectedVals = []
	if (selected !== undefined)
		selectedVals = selected
	var filter = []

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
  g.attr("class", type)
    
  if (type === "degree"){
	  g.attr("transform", `translate(${5*width/6},${height*5/6})`)
		  
	  g.append("text")
	   .text("Degree Distribution")
	   .attr("x", "0.0em")
	   .attr("y", "-6.0em")
	   .style("font-weight", "bold")
	   .style("font-size", "20px")
	   .style("text-anchor", "middle");
   } else if (type === "interior_node"){
	   g.attr("transform", `translate(${width/6},${height*5/6})`)
		  
	  g.append("text")
	   .text("Interior Node Distribution")
	   .attr("x", "0.0em")
	   .attr("y", "-6.0em")
	   .style("font-weight", "bold")
	   .style("font-size", "20px")
	   .style("text-anchor", "middle");
   } else if (type === "interior_edge"){
	   g.attr("transform", `translate(${width/2},${height*5/6})`)
		  
	  g.append("text")
	   .text("Interior Edge Distribution")
	   .attr("x", "0.0em")
	   .attr("y", "-6.0em")
	   .style("font-weight", "bold")
	   .style("font-size", "20px")
	   .style("text-anchor", "middle");
   }
  
  g.selectAll("path")
    .data(arcs)
    .enter().append("path")
	  .attr("d", arc)
	  .attr("id", function(d) { return type+d.data.name })
      .attr("fill", d => pie_color(d.data.name))
      .attr("stroke", "white")
     .on("mouseover", function (d) {
		if(!d3.select(this).attr("clicked")){
			select(d, pie_width, pie_height, circles, innerCircles, data_map, selectedVals, type, this)
			selectedVals.push(parseInt(d.data.name))
			filter = filter.concat(data_map[parseInt(d.data.name)])
			var event = new CustomEvent('highlight_'+type, {detail: {filter:new Set(filter.map(Number)), selected:selectedVals}});
			document.getElementById("graph").dispatchEvent(event);
		}
	})
	.on("mouseout", function(d) {
		if (!d3.select(this).attr("clicked")){
			selectedVals = deselect(d, pie_width, pie_height, circles, innerCircles, data_map, selectedVals, type, this)
			var removalNodes = new Set(data_map[parseInt(d.data.name)])
			filter = filter.filter(e => !removalNodes.has(e))
			var event = new CustomEvent('highlight_'+type, {detail: {filter:new Set(filter.map(Number)), selected:selectedVals}});
			document.getElementById("graph").dispatchEvent(event);
		}
      })
    .on("click", function(d) {
		if(!d3.select(this).attr("clicked")){
			d3.select(this).attr("clicked", true)
			select(d, pie_width, pie_height, circles, innerCircles, data_map, selectedVals, type, this)
			selectedVals.push(parseInt(d.data.name))
			filter = filter.concat(data_map[parseInt(d.data.name)])
			var event = new CustomEvent('highlight_'+type, {detail: {filter:new Set(filter.map(Number)), selected:selectedVals}});
			document.getElementById("graph").dispatchEvent(event);
		} else {
			d3.select(this).attr("clicked", null)
			selectedVals = deselect(d, pie_width, pie_height, circles, innerCircles, data_map, selectedVals, type, this)
			var removalNodes = new Set(data_map[parseInt(d.data.name)])
			filter = filter.filter(e => removalNodes.has(e))
			var event = new CustomEvent('highlight_'+type, {detail: {filter:new Set(filter.map(Number)), selected:selectedVals}});
			document.getElementById("graph").dispatchEvent(event);
		}
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
	
  for (val in selected){
	d3.select("#"+type+selectedVals[val]).each( function(d) {
		select(d, pie_width, pie_height, circles, innerCircles, data_map, selectedVals, type, this)
		d3.select(this).attr("clicked", true)
	})
  }
	
  return svg.node();
}

function select(d, pie_width, pie_height, circles, innerCircles, data_map, selectedVals, type, t){
	d3.select(t).transition()
		.duration(500)
        .attr("d", d3.arc()
			.innerRadius(0)
			.outerRadius(Math.min(pie_width, pie_height) / 2 - 1 + 20));
	var newColor = d3.select(t).attr('fill')
	oldNodes = new Set()
	for (val in selectedVals){
		data_map[selectedVals[val]].map(Number).forEach(item => oldNodes.add(item))
	}
	nodes = new Set(data_map[parseInt(d.data.name)].map(Number))
	if (type === "degree")
		circles.attr("fill", function(d2) { return colorByOutDegree(d2, nodes, oldNodes, newColor, this) })
	else if (type === "interior_node")
		circles.style("stroke", function(d2) { return colorByInteriorNodes(d2, nodes, oldNodes, newColor, this) })
	else if (type === "interior_edge")
		innerCircles.style("fill", function(d2) { return colorByInteriorEdges(d2, nodes, oldNodes, newColor, this) })
}

function deselect(d, pie_width, pie_height, circles, innerCircles, data_map, selectedVals, type, t, interiorInfo){
	d3.select(t).transition()
		.duration(500)
        .attr("d", d3.arc()
			.innerRadius(0)
			.outerRadius(Math.min(pie_width, pie_height) / 2 - 1));	
	selectedVals = selectedVals.filter(e => e != parseInt(d.data.name))
	nodes = new Set(data_map[parseInt(d.data.name)].map(Number))
	if (type === "degree")
		circles.attr("fill", function(d2) { return colorByOutDegree(d2, nodes, oldNodes, d3.rgb("black"), this) })
	else if (type === "interior_node")
		circles.style("stroke", function(d2) { return colorByInteriorNodes(d2, nodes, oldNodes, d3.rgb("black"), this) })
	else if (type === "interior_edge")
		innerCircles.style("fill", function(d2) { return colorByInteriorEdges(d2, nodes, oldNodes, d3.rgb("rgba(0,0,0,0)"), this) })
	return selectedVals
}
