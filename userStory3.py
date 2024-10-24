from neo4j import GraphDatabase
import termtables as tt
import textwrap
from prettytable import PrettyTable
import re 
from itertools import combinations
from concurrent.futures import ThreadPoolExecutor

import time
from tqdm import tqdm
import threading
import progressbar


# URI examples: "neo4j://localhost", "neo4j+s://xxx.databases.neo4j.io"
URI = "bolt://localhost:7687"  
AUTH = ("neo4j", "password") 

driver = GraphDatabase.driver(URI, auth=AUTH)

def check_connection(driver):
    try:
        with driver.session() as session:
            session.run("RETURN 1")
        print("Successfully connected to the database!")
    except Exception as exception:
        print("Failed to connect to the database. Ensure Neo4j is running and credentials/settings are correct.")
        raise exception

#! 从给定的起始节点source_name查询所有可达的目的节点
def find_destinations(driver, source_name):
    
    #! Cypher查询语言，匹配从指定的起始节点到任何目的节点的所有路径 以及 用DISTINCT确保获取节点是唯一
    query = """
    MATCH path = (source:Source {device_name: $source_name})-[*]->(destination:Destination)
    RETURN DISTINCT destination.device_name
    """
    with driver.session() as session:
        result = session.run(query, source_name=source_name)
        destinations = [record['destination.device_name'] for record in result]
        return destinations # 返回一个字符串列表，包含所有从起始节点可以到达的目的节点的名称

#! 对于指定的起始节点，查询到每个目的节点的前5条最短路径
def find_5_shortest_paths_with_exclusion(driver, source_name, destination_names):
    all_paths_info = {}

    def worker(destination_name):
        query = f"""
        MATCH path = (start {{device_name: $source_name}})-[rels:CONNECTS_TO*..10000]->(end {{device_name: $destination_name}})
        WHERE ALL(node IN nodes(path) WHERE node.status = 'Active')
        WITH path, 
             REDUCE(s = 0, r IN rels | s + r.cost) AS relsCost,
             REDUCE(s = 0, node IN nodes(path)[1..-1] | s + node.cost) AS nodesCost
        RETURN path, relsCost + nodesCost AS totalCost
        ORDER BY totalCost ASC
        LIMIT 5
        """

        with driver.session() as session:
            result = session.run(query, source_name=source_name, destination_name=destination_name)
            paths_and_costs = [(record['path'], record['totalCost']) for record in result]

        return destination_name, paths_and_costs

    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(worker, dest_name) for dest_name in destination_names]
        for future in futures:
            destination_name, paths_and_costs = future.result()
            all_paths_info[destination_name] = paths_and_costs

    return all_paths_info

#! 获取用户输入的《有效》起始节点名称
def get_valid_source(driver):
    sources = list_all_source_devices(driver)
    source_name = get_user_input("Enter the source device name (or type 'exit' to quit): ", sources)
    
    # 如果用户输入了一个有效的起始节点，并且该节点有可达的目的节点
    if source_name:
        destinations = find_destinations(driver, source_name)
        if destinations:
            return source_name, destinations # 则返回一个元组（起始节点名称, 可达的目的节点列表）
        else:
            print(f"Device {source_name} does not have any reachable destination devices. Please try another source.")
    
    return None # 否则，返回None

#! 获取用户输入的一个或多个有效目的节点名称
def get_valid_destination(driver, source_name, destinations, selected_destinations):
    
    # 循环提示用户输入一个目的节点名称，直到用户输入'ok'
    while True:
        prompt = f"Enter the destination device name for {source_name} (or type 'ok' to finish or 'exit' to quit): "
        destination_name = get_user_input(prompt, destinations + ['ok'])

        # 如果输入了'ok'
        if destination_name == 'ok':
            print("Selected destinations:", ', '.join(selected_destinations))
            return selected_destinations if selected_destinations else None # 则显示用户已选择的所有目的节点名称

        # 如果输入了一个有效的目的节点名称，将其添加到`selected_destinations`list中，并从`destinations`list中移除
        if destination_name and destination_name in destinations:
            selected_destinations.append(destination_name)
            destinations.remove(destination_name)
        
        # 如果输入了一个无效的目的节点名称，通知并继续提示
        elif destination_name:
            print(f"Invalid input. Please choose a valid option from the list.")

def get_node_cost(driver, node_name):
    query = """
    MATCH (n {device_name: $node_name})
    RETURN n.cost as cost
    """

    with driver.session() as session:
        result = session.run(query, node_name=node_name)
        cost = result.single()["cost"]  # Adapt this line depending on your actual schema
        return cost if cost is not None else 0


#! 计算总路径成本，同时考虑多个路径中可能存在的重叠节点和关系
def calculate_total_path_cost(paths, sub_costs):
    all_nodes = [set([node['device_name'] for node in path.nodes[1:]]) if hasattr(path, 'nodes') and not isinstance(path, str) else set() for path in paths]
    all_edges = [set([(rel.start_node['device_name'], rel.end_node['device_name']) for rel in path.relationships]) if hasattr(path, 'relationships') and not isinstance(path, str) else set() for path in paths]

    visited_nodes = set()
    visited_edges = set()
    overlapping_nodes_cost = 0
    overlapping_edges_cost = 0

    # Compute the cost of overlapping nodes and edges, ensuring each node and edge is only counted once
    for nodes, edges in zip(all_nodes, all_edges):
        for node in nodes:
            if node in visited_nodes:
                overlapping_nodes_cost += get_node_cost(driver, node)
            visited_nodes.add(node)

        for edge in edges:
            if edge in visited_edges:
                overlapping_edges_cost += get_edge_cost(driver, edge)
            visited_edges.add(edge)

    # Exclude the cost of overlapping nodes and edges from the total cost
    total_path_cost = sum(sub_costs) - overlapping_nodes_cost - overlapping_edges_cost

    # print(f"Debug: sub_costs: {sub_costs}, overlapping_nodes_cost: {overlapping_nodes_cost}, overlapping_edges_cost: {overlapping_edges_cost}, total_path_cost: {total_path_cost}")  # Debug print

    return total_path_cost, visited_nodes

def get_edge_cost(driver, edge):
    start_node_name, end_node_name = edge
    query = """
    MATCH (start {device_name: $start_node_name})-[rel:CONNECTS_TO]->(end {device_name: $end_node_name})
    RETURN rel.cost as cost
    """

    with driver.session() as session:
        result = session.run(query, start_node_name=start_node_name, end_node_name=end_node_name)
        cost = result.single()["cost"] 
        return cost if cost is not None else 0

def mark_overlapping_nodes(path_str, overlapped_nodes):
    for node_name in overlapped_nodes:
        # ANSI escape code for bold, italic, and blue text
        marked_text = f"\033[1m\033[3m\033[34m{node_name}\033[0m"
        path_str = path_str.replace(node_name, marked_text)
    return path_str

def interactive_shortest_path(driver):
    while True:
        choice = input("\nEnter 'yes' to search for the shortest path, or type 'exit' to quit: ").strip()
        if choice.lower() == 'exit':
            print("Thank you for using the application. Goodbye!")
            break
        result = get_valid_source(driver)
        if not result:
            continue

        source_name, destinations = result
        selected_destinations = get_valid_destination(driver, source_name, destinations, [])
        if not selected_destinations:
            continue
        
        start_time = time.time()
        
        all_paths_info = find_all_paths_to_destinations(driver, source_name, selected_destinations)

        if all_paths_info:
            print('\nStill calculating...')
            combined_paths_costs = calculate_combined_paths_cost(all_paths_info)

            if combined_paths_costs:
                for idx, (paths, total_cost) in enumerate(combined_paths_costs):
                    paths_str_list = []
                    sub_costs = []
                    for path_info in paths:
                        path, sub_cost = path_info  
                        if isinstance(path, str):
                            path_str = path
                        else:
                            path_str = " -> ".join([node['device_name'] for node in path.nodes])
                        paths_str_list.append(path_str)
                        sub_costs.append(str(sub_cost))  

                    combined_info = list(zip(paths_str_list, sub_costs))
                    paths_str_list = [textwrap.fill(path, width=50) for path, _ in combined_info]
                    sub_costs = [cost for _, cost in combined_info]
                    
                    # 获取重叠的节点
                    overlapping_nodes = find_overlapping_nodes(paths_str_list)

                    # 标记重叠的节点和箭头
                    marked_paths = [mark_overlapping_nodes_and_arrows(path, overlapping_nodes) for path in paths_str_list]

                    #print("\nPath Information:")
                    table = PrettyTable()
                    table.align["Path"] = "l"
                    table.field_names = ["Path #", "Path", "Subpath Cost", "Total Cost"]
                    for i, (path_str, sub_cost) in enumerate(zip(marked_paths, sub_costs)):
                        if i == 0:
                            table.add_row([f"Path {idx+1}", path_str, sub_cost, total_cost])
                        else:
                            table.add_row(["", path_str, sub_cost, ""])
                        table.add_row(["", "", "", ""])


                    print(table)
                    print("\n")
                    end_time = time.time()  # 结束计时
                    elapsed_time = end_time - start_time  # 计算耗时
                print(f"Execution time: {elapsed_time:.4f} seconds")  # 打印耗时
            else:
                print("No paths found.")
        else:
            print("No paths found.")


#! 列出所有的起始节点设备
def list_all_source_devices(driver):
    with driver.session() as session:
        result = session.run("MATCH (n:Source) RETURN n.device_name as device_name")
        sources = [record['device_name'] for record in result]
        return sources

#! 列出从给定起始节点可达的所有目的节点设备
def list_all_destination_devices(driver, source_name):
    query = """
    MATCH (source {device_name: $source_name})-[*]->(destination)
    RETURN DISTINCT destination.device_name as device_name
    """
    with driver.session() as session:
        result = session.run(query, source_name=source_name)
        destinations = [record['device_name'] for record in result]
        return destinations
    
#! 提示用户输入并确保输入是有效的
def get_user_input(prompt, valid_options):
    print("\n" + "=" * 50)
    options_to_display = [opt for opt in valid_options if opt != 'ok'] 
    print("\n".join(options_to_display))
    print("=" * 50 + "\n")
    
    while True:
        user_input = input(prompt).strip()
        if user_input.lower() == 'exit':
            print()
            return None

        if user_input in valid_options or user_input.lower() == 'ok': 
            print() 
            return user_input
        else:
            print(f"Invalid input. Please choose a valid option from the list.")

def set_default_costs(driver):
    # set cost for all nodes depending on their device_type
    set_costs_query = """
    MATCH (n)
    SET n.cost = CASE 
        WHEN n.device_type IN ['Source', 'Destination'] THEN 0
        ELSE 1
    END
    """
    
    with driver.session() as session:
        session.run(set_costs_query)
        # print("Costs have been set according to the device types.")

def path_already_exists(paths_costs, new_path, new_cost):
    for path, cost in paths_costs:
        if cost == new_cost and all(p in path for p in new_path):
            return True
    return False

memo = {}  # 用于存储子问题解的字典

def calculate_combined_paths_cost(all_paths_info, current_path=[], current_cost=0, visited_destinations=set(), memo=None):
    if memo is None:
        memo = {}
    
    if not all_paths_info:
        return [(current_path, current_cost)]

    paths_tuple = tuple((str(p), c) for p, c in current_path)
    memo_key = (current_cost, paths_tuple, frozenset(visited_destinations))

    if memo_key in memo:
        return memo[memo_key]

    combined_paths_costs = []
    for destination_name, paths_and_costs in all_paths_info.items():
        if destination_name in visited_destinations:
            continue

        for path, cost in paths_and_costs:
            if isinstance(path, str):
                continue

            new_path = current_path + [(path, cost)]
            total_cost, _ = calculate_total_path_cost(
                [p for p, _ in new_path], 
                [c for _, c in new_path]
            )

            new_visited_destinations = visited_destinations | {destination_name}
            remaining_destinations = {
                key: val for key, val in all_paths_info.items() 
                if key not in new_visited_destinations
            }

            for sub_path, sub_cost in calculate_combined_paths_cost(
                remaining_destinations, new_path, total_cost, new_visited_destinations, memo
            ):
                if not path_already_exists(combined_paths_costs, sub_path, sub_cost):
                    combined_paths_costs.append((sub_path, sub_cost))

    combined_paths_costs.sort(key=lambda x: x[1])
    memo[memo_key] = combined_paths_costs[:5]

    return combined_paths_costs[:5]


def find_all_paths_to_destinations(driver, source_name, destination_names):
    all_paths_info = {}
    
    for destination_name in destination_names:
        query = f"""
        MATCH path = (start {{device_name: $source_name}})-[rels:CONNECTS_TO*..10000]->(end {{device_name: $destination_name}})
        WHERE ALL(node IN nodes(path) WHERE node.status = 'Active')
        WITH path, 
             REDUCE(s = 0, r IN rels | s + r.cost) AS relsCost,
             REDUCE(s = 0, node IN nodes(path)[1..-1] | s + node.cost) AS nodesCost
        RETURN path, relsCost + nodesCost AS totalCost
        ORDER BY totalCost ASC
        LIMIT 10
        """

        with driver.session() as session:
            result = session.run(query, source_name=source_name, destination_name=destination_name)
            paths_and_costs = [(record['path'], record['totalCost']) for record in result]

        if paths_and_costs:
            all_paths_info[destination_name] = paths_and_costs
    
    return all_paths_info

def find_overlapping_nodes(paths):
    # print(f"Debug: paths = {paths}")  # Print the paths list for debugging
    nodes = [re.findall(r'\b\w+\b', path) for path in paths]
    # print(f"Debug: nodes = {nodes}")  # Print the nodes list for debugging

    # 检查nodes列表中是否有至少两个元素
    if len(nodes) < 2:
        # print("Debug: Less than two paths found.")  # Print a debug message if less than two paths are found
        return set()

    overlapping_nodes = set(nodes[0]).intersection(set(nodes[1]))
    # print(f"Debug: overlapping_nodes = {overlapping_nodes}")  # Print the overlapping nodes for debugging
    return overlapping_nodes


def mark_overlapping_nodes_and_arrows(path_str, overlapped_nodes):
    marked_path_str = path_str
    for node_name in overlapped_nodes:
        pattern = re.compile(f'\\b{node_name}\\b( ->)?|(-> )?\\b{node_name}\\b')
        marked_text = lambda m: f"\033[1m\033[3m\033[34m{m.group()}\033[0m"
        marked_path_str = re.sub(pattern, marked_text, marked_path_str)
    return marked_path_str

def main():
    print("Checking database connection...")
    check_connection(driver)
    set_default_costs(driver)
    interactive_shortest_path(driver)

if __name__ == '__main__':
    main()