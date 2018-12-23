
import random 


def generate_orders(number):
    oid_list = []
    quantity_list = []
    for i in range(number):
        oid_list.append(random.randint(4000000,86432660))
        quantity_list.append(random.randint(1,70000))
    oid_list.sort()
    return oid_list,quantity_list

def write_to_file(orders,quantities):
    with open('orders.csv','a+') as file:
        for order, quantity in zip(orders, quantities) :
            file.write('OID' + str(order) + ',' + str(quantity))
            file.write('\n')
    file.close()


if __name__ == "__main__":
    no_of_orders = 2000
    order_ids, quant_list = generate_orders(no_of_orders)
    write_to_file(order_ids,quant_list)
