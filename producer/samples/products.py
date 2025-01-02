from faker.providers.python import Provider as PythonProvider

class Provider(PythonProvider):
    product_names = (
        "Ball",
        "Soap",
        "Keyboard",
        "Cheese",
        "Car",
        "Shirt",
        "Chips",
        "Hat",
        "Tuna"
    )
    product_adjectives = (
        "Luxurious",
        "Incredible",
        "Refined",
        "Practical",
        "Handcrafted",
        "Licensed",
        "Incredible",
        "Rustic",
        "Licensed",
        "Modern"
    )
    product_materials = (
        "Concrete",
        "Cotton",
        "Wooden",
        "Metal",
        "Rubber"
    )
    product_categories = (
        "Clothing and Apparel",
        "Home Appliances",
        "Electronics",
        "Furniture",
        "Automotive",
        "Books and Media",
        "Food and Beverages",
        "Health and Beauty",
        "Sports and Outdoors",
        "Toys and Games"
    )
    product_subcategories = (
        "Mobile Phones & Accessories",
        "Refrigerators",
        "Living Room Furniture",
        "Men's Wear",
        "Skincare Products",
        "Fitness Equipment",
        "Action Figures",
        "Vehicle Parts and Accessories",
        "Fiction and Non-Fiction Books",
        "Groceries"
    )
    product_descriptions = (
        "Boston's most advanced compression wear technology increases muscle oxygenation, stabilizes active muscles",
        "The slim & simple Maple Gaming Keyboard from Dev Byte comes with a sleek body and 7- Color RGB LED Back-lighting for smart functionality",
        "Andy shoes are designed to keeping in mind durability as well as trends, the most stylish range of shoes & sandals",
        "The Football Is Good For Training And Recreational Purposes",
        "New range of formal shirts are designed keeping you in mind. With fits and styling that will make you stand apart",
        "Ergonomic executive chair upholstered in bonded black leather and PVC padded seat and back for all-day comfort and support",
        "The beautiful range of Apple Naturalé that has an exciting mix of natural ingredients. With the Goodness of 100% Natural Ingredients",
        "The Apollotech B340 is an affordable wireless mouse with reliable connectivity, 12 months battery life and modern design"
    )

    def product_name(self) -> str:
        return self.random_element(self.product_names)

    def product_adjective(self) -> str:
        return self.random_element(self.product_adjectives)

    def product_material(self) -> str:
        return self.random_element(self.product_materials)

    def product_category(self) -> str:
        return self.random_element(self.product_categories)

    def product_subcategory(self) -> str:
        return self.random_element(self.product_subcategories)

    def product_description(self) -> str:
        return self.random_element(self.product_descriptions)