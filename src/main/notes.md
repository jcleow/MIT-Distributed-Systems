Context:
For intermediate files mr-X-Y

X is the Map TaskID 
and Y is the reduce Bucket

Map Task: Each worker works on 1 X and multiple Ys. Each X also corresponds to a particular file
Reduce Task: Each worker works on 1Y across multiple Xs

		Map Task	Map Task	Map task
			|			|			|
			V			V			V
0		mr-0-0		mr-1-0		mr-2-0 -> Reduce task
1		mr-0-1		mr-1-1		mr-2-1 -> Reduce task
2		mr-0-2		mr-1-2		mr-2-2 -> Reduce task

