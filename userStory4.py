from neo4j import GraphDatabase
import itertools
import termtables as tt
import textwrap
from prettytable import PrettyTable


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
    
    #! 使用Cypher查询语言，匹配从指定的起始节点到任何目的节点的所有路径 以及 用DISTINCT确保获取节点是唯一
    query = """
    MATCH path = (source:Source {device_name: $source_name})-[*]->(destination:Destination)
    RETURN DISTINCT destination.device_name
    """
    with driver.session() as session:
        result = session.run(query, source_name=source_name)
        destinations = [record['destination.device_name'] for record in result]
        return destinations # 返回一个字符串列表，包含所有从起始节点可以到达的目的节点的名称

# 检查从给定的起始节点到目的节点之间是否存在活动路径
# def check_path_existence(driver, source_name, destination_name):
    
#     # 匹配从指定的起始节点到指定的目的节点的所有路径 以及 确保路径中的所有节点都是active的
#     query = """
#     MATCH path = (start {device_name: $source_name})-[:CONNECTS_TO*..1000]->(end {device_name: $destination_name})
#     WHERE ALL(node IN nodes(path) WHERE node.status = 'Active')
#     RETURN count(path) as pathCount
#     """
    
#     with driver.session() as session:
#         result = session.run(query, source_name=source_name, destination_name=destination_name)
#         path_count = result.single().get("pathCount")
#         return path_count > 0 # 返回布尔值。如果存在至少一个活动路径，则返回True，否则返回False。

#! 对于指定的起始节点，查询到每个目的节点的前5条最短路径
def find_5_shortest_paths_with_exclusion(driver, source_name, destination_names):
    
    # 初始化一个字典`all_paths_info`来保存每个目的节点的前5条最短路径
    # 键是目的节点的名称，值是一个包含5个元组的列表。每个元组包含两个元素：
    # - 路径：表示从起始节点到目的节点的路径。
    # - 总成本：表示路径的总成本。
    
    all_paths_info = {} 
    
    for destination_name in destination_names:
        # if not check_path_existence(driver, source_name, destination_name):
        #     print(f"No active path exists between {source_name} and {destination_name}.")
        #     continue
        
        #  1. 查询从指定的起始节点到当前目的节点的所有路径。
        #  2. 通过计算路径上的关系和节点的成本，得到每条路径的总成本。
        #  3. 按总成本对结果进行排序，并只选择前5条最短的路径。
        
        query = f"""
        MATCH path = (start {{device_name: $source_name}})-[rels:CONNECTS_TO*..10000]->(end {{device_name: $destination_name}})
        WHERE ALL(node IN nodes(path) WHERE node.status = 'Active')
        WITH path, nodes(path) AS nodes, rels, 
             REDUCE(s = 0, r IN rels | s + r.cost) AS relsCost
        WITH path, nodes, relsCost, 
             REDUCE(s = relsCost, node IN nodes[1..-1] | s + node.cost) AS totalCost
        ORDER BY totalCost ASC
        LIMIT 5
        RETURN path, totalCost
        """
        
        with driver.session() as session:
            result = session.run(query, source_name=source_name, destination_name=destination_name)
            paths_and_costs = [(record['path'], record['totalCost']) for record in result]
        
        #!  4. 如果查询结果的数量小于5，将剩余的位置填充为默认值（表示没有可用的路径）。
        while len(paths_and_costs) < 5:
            paths_and_costs.append(("没有可用的路径fk", 0))
        
        #!  5. 将当前目的节点的前5条最短路径保存到`all_paths_info`字典中。
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

#! 计算总路径成本，同时考虑多个路径中可能存在的重叠节点和关系
def calculate_total_path_cost(paths, sub_costs):
    # 简单说就是从每个路径中提取其节点和关系的信息，并将这些信息存储为集合
    all_nodes = [set([(node['device_name'], node['cost']) for node in path.nodes[1:]]) if hasattr(path, 'nodes') and not isinstance(path, str) else set() for path in paths]
    all_rels = [set([(rel.start_node['device_name'], rel.end_node['device_name'], rel['cost']) for rel in path.relationships]) if hasattr(path, 'relationships') and not isinstance(path, str) else set() for path in paths]
    
    # 计算所有路径中重叠的关系。如果只有一个路径，则没有重叠的节点和边！
    overlapping_nodes = set.intersection(*all_nodes) if len(all_nodes) > 1 else set()
    overlapping_rels = set.intersection(*all_rels) if len(all_rels) > 1 else set()
    
    # 计算重叠节点 和 边的总成本
    overlapping_nodes_cost = sum([node_cost for _, node_cost in overlapping_nodes]) if overlapping_nodes else 0
    overlapping_rels_cost = sum([rel_cost for _, _, rel_cost in overlapping_rels]) if overlapping_rels else 0
    total_overlapping_cost = overlapping_nodes_cost + overlapping_rels_cost
    #! 从子路径的总成本中减去重叠的成本，得到最终的总路径成本
    total_path_cost = sum(sub_costs) - total_overlapping_cost
    
    return total_path_cost, {node_name for node_name, _ in overlapping_nodes}

# 标记重复点 没啥好说的 加粗上色
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
        
        if choice.lower() == 'yes':
            # 获取源设备
            result = get_valid_source(driver)
            # 返回的源设备和可能的目的地
            source_name, destinations = result
            selected_destinations = []
            # 获取用户选择的目的地
            destination_names = get_valid_destination(driver, source_name, destinations, selected_destinations)
            if destination_names is None:
                continue
            
            #! 计算 源和目的地获取5条最短路径，关联的成本
            all_paths_info = find_5_shortest_paths_with_exclusion(driver, source_name, destination_names)

            # 如果找到了路径
            if all_paths_info:
                print("\nPaths Information:")
                # 遍历前5条路径。
                for idx in range(5):
                    paths_data = [all_paths_info[dest][idx] for dest in destination_names]
                    # 将路径数据分为路径，关联的子成本
                    paths_list, sub_costs = zip(*[(record[0], record[1]) for record in paths_data])
                    
                    #! 考虑重叠计算路径的总成本
                    total_path_cost, overlapping_nodes = calculate_total_path_cost(paths_list, sub_costs)
                    
                    # 将路径数据转换为字符串表示形式。
                    paths_str_list = []
                    for path in paths_list:
                        if isinstance(path, str):
                            path_str = path
                        else:
                            path_str = " -> ".join([node['device_name'] for node in path.nodes])
                        path_str = mark_overlapping_nodes(path_str, overlapping_nodes) #! 标记重复节点
                        paths_str_list.append(path_str)

                    # 格式化显示的路径字符串。
                    paths_str_list = [textwrap.fill(path, width=50) for path in paths_str_list]

                    # 初始化表格 和 显示路径及其成本。
                    table = PrettyTable()
                    table.align["Path"] = "l"
                    table.field_names = ["Path #", "Path", "Sub Path Cost", "Path Cost"] 

                    #! 使用路径数据填充表格
                    for i, (path_str, sub_cost) in enumerate(zip(paths_str_list, sub_costs)):
                        if i == 0:
                            table.add_row([f"Path {idx+1}", path_str, sub_cost, total_path_cost]) 
                        else:
                            table.add_row(["", path_str, sub_cost, ""])
                        table.add_row(["", "", "", ""]) 

                    print(table)
                    print("\n") 
                else:
                    print("No paths found.")
            else:
                print("Invalid choice, please try again.")

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

def main():
    print("Checking database connection...")
    check_connection(driver)
    set_default_costs(driver)
    interactive_shortest_path(driver)

if __name__ == '__main__':
    main()