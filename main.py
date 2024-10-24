from neo4j import GraphDatabase

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

def print_all_nodes(driver):
    with driver.session() as session:
        result = session.run("MATCH (n) RETURN n")
        for record in result:
            node = record['n']
            print(node)

# Find all destinations reached by source_name
def find_destinations(driver, source_name):
    query = """
    MATCH path = (source:Source {device_name: $source_name})-[*]->(destination:Destination)
    RETURN DISTINCT destination.device_name
    """
    with driver.session() as session:
        result = session.run(query, source_name=source_name)
        destinations = [record['destination.device_name'] for record in result]
        return destinations

def check_path_existence(driver, source_name, destination_name):
    query = """
    MATCH path = (start {device_name: $source_name})-[:CONNECTS_TO*..1000]->(end {device_name: $destination_name})
    WHERE ALL(node IN nodes(path) WHERE node.status = 'Active')
    RETURN count(path) as pathCount
    """
    
    with driver.session() as session:
        result = session.run(query, source_name=source_name, destination_name=destination_name)
        path_count = result.single().get("pathCount")
        return path_count > 0
    
def find_k_shortest_paths_with_exclusion(driver, source_name, destination_name, k, excluded_devices):
    if not check_path_existence(driver, source_name, destination_name):
        print(f"No active path exists between {source_name} and {destination_name}.")
        return 
    
    excluded_string = ', '.join(f"'{device}'" for device in excluded_devices)
    
    query = f"""
    MATCH path = (start {{device_name: $source_name}})-[rels:CONNECTS_TO*..10000]->(end {{device_name: $destination_name}})
    WHERE ALL(node IN nodes(path) WHERE node.status = 'Active' AND NOT node.device_name IN [{excluded_string}])
    WITH path, nodes(path) AS nodes, rels, REDUCE(s = 0, r IN rels | s + r.cost) AS relsCost
    WITH path, nodes, relsCost, REDUCE(s = relsCost, node IN nodes | s + node.cost) AS totalCost
    ORDER BY totalCost ASC
    LIMIT $k
    RETURN path, totalCost
    """
    
    with driver.session() as session:
        result = session.run(query, source_name=source_name, destination_name=destination_name, k=k)
        paths = [(record['path'], record['totalCost']) for record in result]

        if paths:
            print(f"\n{len(paths)} Shortest Path(s) Found:")
            for idx, (path, totalCost) in enumerate(paths):
                device_names = " -> ".join([node['device_name'] for node in path.nodes])
                print(f"Path {idx + 1}:")
                print(f"Total Cost: {totalCost}")
                print(device_names + '\n')
        else:
            print("No paths found.\n")

def get_valid_source(driver):
    sources = list_all_source_devices(driver)
    source_name = get_user_input("Enter the source device name (or type 'exit' to quit): ", sources)
    if source_name:
        destinations = find_destinations(driver, source_name)
        if destinations:
            return source_name, destinations
        else:
            print(f"Device {source_name} does not have any reachable destination devices. Please try another source.")
    return None

def get_valid_destination(driver, source_name, destinations):
    prompt = f"Enter the destination device name for {source_name} (or type 'exit' to quit): "
    destination_name = get_user_input(prompt, destinations)
    return destination_name

def interactive_shortest_path(driver):
    while True:
        choice = input("\nEnter 'yes' to search for the shortest path, or type 'exit' to quit: ").strip()
        if choice.lower() == 'exit':
            print("Thank you for using the application. Goodbye!")
            break
        
        if choice == 'yes':
            result = get_valid_source(driver)
            if result is None:
                continue
            
            source_name, destinations = result

            destination_name = get_valid_destination(driver, source_name, destinations)
            if destination_name is None:
                continue
            
            while True:
                try:
                    k = int(input("Enter the number of shortest paths to find: ").strip())
                    if k > 0:
                        break
                    else:
                        print("Please enter a positive integer.")
                except ValueError:
                    print("Invalid input. Please enter a positive integer.")
            
            exclude_choice = input("Do you want to exclude any devices? Enter 'yes' to exclude or 'no' to continue without excluding: ").strip().lower()
            if exclude_choice == 'yes':
                excluded_devices = input("Enter the devices to be excluded, separated by commas: ").strip().split(',')
                find_k_shortest_paths_with_exclusion(driver, source_name, destination_name, k, excluded_devices)
            elif exclude_choice == 'no':
                find_k_shortest_paths_with_exclusion(driver, source_name, destination_name, k, [])
            else:
                print("Invalid choice, please enter 'yes' or 'no'.")
        else:
            print("Invalid choice, please try again.")

def list_all_source_devices(driver):
    with driver.session() as session:
        result = session.run("MATCH (n:Source) RETURN n.device_name as device_name")
        sources = [record['device_name'] for record in result]
        return sources

def list_all_destination_devices(driver, source_name):
    query = """
    MATCH (source {device_name: $source_name})-[*]->(destination)
    RETURN DISTINCT destination.device_name as device_name
    """
    with driver.session() as session:
        result = session.run(query, source_name=source_name)
        destinations = [record['device_name'] for record in result]
        return destinations

def get_user_input(prompt, valid_options):
    print("\n" + "=" * 50)
    print("\n".join(valid_options))
    print("=" * 50 + "\n")
    
    while True:
        user_input = input(prompt).strip()
        if user_input.lower() == 'exit':
            print()
            return None

        if user_input in valid_options:
            print() 
            return user_input
        else:
            print(f"Invalid input. Please choose a valid option from the list.")

def set_default_costs(driver):
    # Set cost for all nodes depending on their device_type
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