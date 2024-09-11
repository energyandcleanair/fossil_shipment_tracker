import random
import string


class DetailsGenerator:
    def generate_name(self):
        first_names = ["John", "Jane", "Alex", "Emily", "Chris", "Katie", "Michael", "Sarah"]
        last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis"]
        return f"{random.choice(first_names)} {random.choice(last_names)}"

    # Function to generate a random address line
    def generate_address_line(self):
        street_names = ["Main St", "High St", "Park Ave", "Oak St", "Maple St", "Cedar Ave"]
        street_number = random.randint(1, 9999)
        return f"{street_number} {random.choice(street_names)}"

    # Function to generate a random city
    def generate_city(self):
        cities = [
            "New York",
            "Los Angeles",
            "Chicago",
            "Houston",
            "Phoenix",
            "Philadelphia",
            "San Antonio",
        ]
        return random.choice(cities)

    # Function to generate a random postal code
    def generate_post_code(self):
        return "".join(random.choices(string.ascii_uppercase + string.digits, k=5))


class PasswordGenerator:
    def __init__(self, length: int = 12):
        self.length = length

    def generate_password(self):
        # Define the character sets
        lower = string.ascii_lowercase
        upper = string.ascii_uppercase
        digits = string.digits
        special_chars = string.punctuation

        # Combine all character sets
        all_chars = lower + upper + digits + special_chars

        # Ensure the password contains at least one character from each set
        password = [
            random.choice(lower),
            random.choice(upper),
            random.choice(digits),
            random.choice(special_chars),
        ]

        # Fill the rest of the password length with random choices from all_chars
        password += random.choices(all_chars, k=self.length - 4)

        # Shuffle the password to prevent predictable patterns
        random.shuffle(password)

        # Convert list to string
        return "".join(password)
