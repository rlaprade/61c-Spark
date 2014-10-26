from pyspark import SparkContext
import Sliding, argparse

def get_level(state):
    """Abstraction for obtaining the level of a 
    state representation
    """
    return state[1]
    
def get_board(state):
    """Abstraction for obtaining the board of a 
    state representation
    """
    return state[0]
    
def make_state(level, board):
    """Abstraction for making a state wher 
    level and board are represented    
    """
    return (board, level)

def bfs_flat_map(state):
    """ """
    self_list = [state]
    if get_level(state) == level-1:
        #expand children if state is on current (highest) level
        children = Sliding.children(WIDTH, HEIGHT, get_board(state))
        return [make_state(level, board) for board in children] + self_list
    return self_list
        
def bfs_map(child):
    """ """
    pass
    
def bfs_reduce(lvl1, lvl2):
    """Sets level for each state to minimum"""
    return min(lvl1, lvl2)

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
    level_pos = sc.parallelize((make_state(level, sol),))
    prev_size, size = 0, 1
    
    while prev_size != size:
        level += 1
        level_pos = level_pos.flatMap(bfs_flat_map) \
                 .reduceByKey(bfs_reduce)
                 # .map(bfs_map) \
        prev_size = size
        size = level_pos.count()
        # output("level: {}, size: {}, prev_size: {}".format(level, size, prev_size))
        # output(str(level_pos.collect()))

    """ YOUR OUTPUT CODE HERE """
    # Get a from level_pos and sort it by level
    state_space = sorted(level_pos.collect(), key=lambda state: get_level(state))
    for state in state_space:
        output("{lvl} {brd}".format(lvl=get_level(state), brd=get_board(state)))
    
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
