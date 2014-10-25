from pyspark import SparkContext
import Sliding, argparse


def bfs_flat_map(state):
    """Flat map expands the children"""
    return Sliding.children(WIDTH, HEIGHT, state)

def map_maker(explored_set):
    """Returns the map function which can read the current explored set"""
    def bfs_map(child):
        """Map singles out children and checks if explored"""
        if child not in explored_set:
            return set((tuple(child),))
        return set()
    return bfs_map

def bfs_reduce(state1, state2):
    """Reduce combines into states into a single set
    whilch will be come the new frontier and is also
    combined with explored set
    """
    return state1 | state2

def solve_sliding_puzzle(master, output, height, width):
    """
    Solves a sliding puzzle of the provided height and width.
     master: specifies master url for the spark context
     output: function that accepts string to write to the output file
     height: height of puzzle
     width: width of puzzle
    """
    # Set up the spark context. Use this to create your RDD
    sc = SparkContext(master, "python")

    # Global constants that will be shared across all map and reduce instances.
    # You can also reference these in any helper functions you write.
    global HEIGHT, WIDTH, level

    # Initialize global constants
    HEIGHT=height
    WIDTH=width
    level = 0 # this "constant" will change, but it remains constant for every MapReduce job

    # The solution configuration for this sliding puzzle. You will begin exploring the tree from this node
    sol = Sliding.solution(WIDTH, HEIGHT)
    

    """ YOUR MAP REDUCE PROCESSING CODE HERE """
    frontier = [sol,]
    explored = set(frontier)
    level_pos = sc.parallelize(((level, sol),))
    
    while frontier:
        level += 1
        bfs_map = map_maker(explored)
        frontier_rdd = sc.parallelize(frontier)
        newly_found = frontier_rdd.flatMap(bfs_flat_map) \
                 .map(bfs_map) \
                 .reduce(bfs_reduce)
                 
        # output(str(type(newly_found)))
        # output(str(newly_found))
                 
        explored |= newly_found
        frontier = newly_found
        new_rdd = sc.parallelize([(level, state) for state in newly_found])
        level_pos = level_pos.union(new_rdd)

    """ YOUR OUTPUT CODE HERE """
    for entry in level_pos.collect():
        output(str(entry[0]) + " " + str(entry[1]))
    
    sc.stop()



""" DO NOT EDIT PAST THIS LINE

You are welcome to read through the following code, but you
do not need to worry about understanding it.
"""

def main():
    """
    Parses command line arguments and runs the solver appropriately.
    If nothing is passed in, the default values are used.
    """
    parser = argparse.ArgumentParser(
            description="Returns back the entire solution graph.")
    parser.add_argument("-M", "--master", type=str, default="local[8]",
            help="url of the master for this job")
    parser.add_argument("-O", "--output", type=str, default="solution-out",
            help="name of the output file")
    parser.add_argument("-H", "--height", type=int, default=2,
            help="height of the puzzle")
    parser.add_argument("-W", "--width", type=int, default=2,
            help="width of the puzzle")
    args = parser.parse_args()


    # open file for writing and create a writer function
    output_file = open(args.output, "w")
    writer = lambda line: output_file.write(line + "\n")

    # call the puzzle solver
    solve_sliding_puzzle(args.master, writer, args.height, args.width)

    # close the output file
    output_file.close()

# begin execution if we are running this file directly
if __name__ == "__main__":
    main()
