#!/usr/bin/env python3
"""
Generate restaurant menu PDFs for Casper's Kitchens document processing demo.

Uses the existing Casper's brands and enriches them with nutritional information
and allergen data. Outputs:
  - data/menus/pdfs/*.pdf  (one per brand)
  - data/menus/menu_metadata.json  (structured source data)

Requirements:
  pip install fpdf2
"""

import json
import os
import textwrap
from pathlib import Path

from fpdf import FPDF

SCRIPT_DIR = Path(__file__).parent
PDF_DIR = SCRIPT_DIR / "pdfs"
METADATA_PATH = SCRIPT_DIR / "menu_metadata.json"

# ---------------------------------------------------------------------------
# Allergen codes used across all menus
# ---------------------------------------------------------------------------
ALLERGEN_LEGEND = {
    "wheat": "Wheat / Gluten",
    "milk": "Milk / Dairy",
    "egg": "Eggs",
    "soy": "Soy",
    "peanut": "Peanuts",
    "tree_nut": "Tree Nuts",
    "fish": "Fish",
    "shellfish": "Shellfish",
    "sesame": "Sesame",
}

# ---------------------------------------------------------------------------
# Brand definitions with menu items, nutrition & allergen data
# ---------------------------------------------------------------------------
BRANDS = [
    {
        "brand_name": "Wok This Way",
        "tagline": "Stir-fried to perfection",
        "cuisine": "Asian",
        "items": [
            {"name": "Beef & Broccoli Stir Fry", "description": "Tender beef and crisp broccoli in savory soy sauce, served over steamed rice.", "category": "Mains", "price": 15.99, "calories": 520, "protein_g": 34, "fat_g": 18, "carbs_g": 55, "allergens": ["soy"]},
            {"name": "Vegetable Fried Rice", "description": "Day-old rice wok-tossed with seasonal vegetables, egg, and soy sauce.", "category": "Mains", "price": 12.99, "calories": 440, "protein_g": 12, "fat_g": 14, "carbs_g": 64, "allergens": ["soy", "egg"]},
            {"name": "Kung Pao Chicken", "description": "Spicy diced chicken with peanuts, chili peppers, and Sichuan peppercorns.", "category": "Mains", "price": 14.99, "calories": 480, "protein_g": 30, "fat_g": 22, "carbs_g": 38, "allergens": ["soy", "peanut"]},
            {"name": "Shrimp Lo Mein", "description": "Egg noodles stir-fried with jumbo shrimp and Asian vegetables.", "category": "Mains", "price": 16.49, "calories": 560, "protein_g": 28, "fat_g": 16, "carbs_g": 72, "allergens": ["wheat", "soy", "shellfish", "egg"]},
            {"name": "Spring Rolls (4 pc)", "description": "Crispy vegetable spring rolls with sweet chili dipping sauce.", "category": "Appetizers", "price": 7.99, "calories": 280, "protein_g": 6, "fat_g": 14, "carbs_g": 32, "allergens": ["wheat", "soy"]},
            {"name": "Wonton Soup", "description": "Pork and shrimp wontons in aromatic chicken broth with bok choy.", "category": "Appetizers", "price": 8.49, "calories": 210, "protein_g": 14, "fat_g": 8, "carbs_g": 20, "allergens": ["wheat", "soy", "shellfish"]},
            {"name": "Mango Sticky Rice", "description": "Sweet coconut sticky rice topped with fresh mango slices.", "category": "Desserts", "price": 6.99, "calories": 340, "protein_g": 4, "fat_g": 8, "carbs_g": 64, "allergens": ["milk"]},
            {"name": "Jasmine Tea", "description": "Fragrant hot jasmine green tea.", "category": "Drinks", "price": 2.99, "calories": 0, "protein_g": 0, "fat_g": 0, "carbs_g": 0, "allergens": []},
            {"name": "Thai Iced Tea", "description": "Sweetened black tea with condensed milk over ice.", "category": "Drinks", "price": 4.49, "calories": 180, "protein_g": 2, "fat_g": 4, "carbs_g": 36, "allergens": ["milk"]},
        ],
    },
    {
        "brand_name": "Pho Real",
        "tagline": "Authentic Vietnamese flavors",
        "cuisine": "Asian",
        "items": [
            {"name": "Classic Beef Pho", "description": "Rich bone broth with rice noodles, rare beef slices, and fresh herbs.", "category": "Mains", "price": 14.99, "calories": 450, "protein_g": 32, "fat_g": 10, "carbs_g": 52, "allergens": ["soy", "fish"]},
            {"name": "Chicken Pho", "description": "Clear chicken broth with rice noodles, poached chicken, and bean sprouts.", "category": "Mains", "price": 13.99, "calories": 380, "protein_g": 28, "fat_g": 6, "carbs_g": 50, "allergens": ["soy"]},
            {"name": "Banh Mi Sandwich", "description": "Crispy baguette with lemongrass pork, pickled daikon, cilantro, and jalapeno.", "category": "Mains", "price": 11.49, "calories": 520, "protein_g": 24, "fat_g": 18, "carbs_g": 62, "allergens": ["wheat", "soy", "egg"]},
            {"name": "Vermicelli Bowl", "description": "Rice vermicelli with grilled lemongrass chicken, fresh vegetables, and nuoc cham.", "category": "Mains", "price": 13.49, "calories": 410, "protein_g": 26, "fat_g": 8, "carbs_g": 58, "allergens": ["fish", "soy"]},
            {"name": "Fresh Spring Rolls (2 pc)", "description": "Rice paper rolls with shrimp, herbs, vermicelli, and peanut dipping sauce.", "category": "Appetizers", "price": 7.49, "calories": 190, "protein_g": 10, "fat_g": 4, "carbs_g": 28, "allergens": ["shellfish", "peanut"]},
            {"name": "Crispy Imperial Rolls (3 pc)", "description": "Deep-fried pork and vegetable rolls with fish sauce.", "category": "Appetizers", "price": 8.99, "calories": 320, "protein_g": 12, "fat_g": 18, "carbs_g": 28, "allergens": ["wheat", "fish", "egg"]},
            {"name": "Vietnamese Iced Coffee", "description": "Strong drip coffee with sweetened condensed milk over ice.", "category": "Drinks", "price": 4.99, "calories": 160, "protein_g": 2, "fat_g": 4, "carbs_g": 28, "allergens": ["milk"]},
            {"name": "Coconut Smoothie", "description": "Creamy coconut milk blended with ice.", "category": "Drinks", "price": 5.49, "calories": 220, "protein_g": 2, "fat_g": 12, "carbs_g": 26, "allergens": ["tree_nut"]},
        ],
    },
    {
        "brand_name": "Thai One On",
        "tagline": "Bold Thai street food",
        "cuisine": "Asian",
        "items": [
            {"name": "Pad Thai", "description": "Stir-fried rice noodles with shrimp, egg, bean sprouts, peanuts, and tamarind sauce.", "category": "Mains", "price": 14.99, "calories": 510, "protein_g": 24, "fat_g": 16, "carbs_g": 68, "allergens": ["shellfish", "egg", "peanut", "soy", "fish"]},
            {"name": "Green Curry Chicken", "description": "Creamy coconut green curry with chicken, Thai basil, and bamboo shoots.", "category": "Mains", "price": 15.49, "calories": 480, "protein_g": 28, "fat_g": 24, "carbs_g": 38, "allergens": ["fish", "milk"]},
            {"name": "Massaman Curry", "description": "Rich peanut-based curry with slow-braised beef, potatoes, and onions.", "category": "Mains", "price": 16.49, "calories": 580, "protein_g": 32, "fat_g": 30, "carbs_g": 44, "allergens": ["peanut", "fish", "milk"]},
            {"name": "Basil Fried Rice", "description": "Wok-fried jasmine rice with Thai basil, chili, and choice of protein.", "category": "Mains", "price": 13.49, "calories": 460, "protein_g": 20, "fat_g": 14, "carbs_g": 62, "allergens": ["soy", "egg", "fish"]},
            {"name": "Tom Yum Soup", "description": "Hot and sour shrimp soup with lemongrass, galangal, and lime leaves.", "category": "Appetizers", "price": 8.99, "calories": 180, "protein_g": 14, "fat_g": 6, "carbs_g": 16, "allergens": ["shellfish", "fish"]},
            {"name": "Satay Skewers (4 pc)", "description": "Grilled chicken skewers with peanut dipping sauce and cucumber relish.", "category": "Appetizers", "price": 9.49, "calories": 340, "protein_g": 22, "fat_g": 18, "carbs_g": 20, "allergens": ["peanut", "soy"]},
            {"name": "Mango Lassi", "description": "Creamy yogurt blended with ripe mango.", "category": "Drinks", "price": 4.99, "calories": 200, "protein_g": 4, "fat_g": 4, "carbs_g": 38, "allergens": ["milk"]},
            {"name": "Coconut Ice Cream", "description": "House-made coconut ice cream with crushed peanuts.", "category": "Desserts", "price": 5.99, "calories": 260, "protein_g": 4, "fat_g": 16, "carbs_g": 28, "allergens": ["tree_nut", "peanut", "milk"]},
        ],
    },
    {
        "brand_name": "Taco 'Bout It",
        "tagline": "Street tacos done right",
        "cuisine": "Mexican",
        "items": [
            {"name": "Chicken Tacos (3 pc)", "description": "Soft corn tortillas with seasoned grilled chicken, cilantro, and onion.", "category": "Mains", "price": 12.99, "calories": 420, "protein_g": 30, "fat_g": 14, "carbs_g": 42, "allergens": []},
            {"name": "Carne Asada Tacos (3 pc)", "description": "Chargrilled steak on corn tortillas with salsa verde and queso fresco.", "category": "Mains", "price": 14.49, "calories": 480, "protein_g": 34, "fat_g": 20, "carbs_g": 40, "allergens": ["milk"]},
            {"name": "Fish Tacos (3 pc)", "description": "Beer-battered cod with cabbage slaw, chipotle crema, and lime.", "category": "Mains", "price": 14.99, "calories": 510, "protein_g": 26, "fat_g": 22, "carbs_g": 52, "allergens": ["fish", "wheat", "milk", "egg"]},
            {"name": "Vegetarian Quesadilla", "description": "Grilled flour tortilla with melted cheese, black beans, and peppers.", "category": "Mains", "price": 10.99, "calories": 440, "protein_g": 18, "fat_g": 22, "carbs_g": 42, "allergens": ["wheat", "milk"]},
            {"name": "Guacamole & Chips", "description": "Fresh-made guacamole with house-fried tortilla chips.", "category": "Appetizers", "price": 8.49, "calories": 320, "protein_g": 4, "fat_g": 22, "carbs_g": 30, "allergens": []},
            {"name": "Elote (Street Corn)", "description": "Grilled corn with mayo, cotija cheese, chili powder, and lime.", "category": "Sides", "price": 5.99, "calories": 260, "protein_g": 6, "fat_g": 14, "carbs_g": 32, "allergens": ["milk", "egg"]},
            {"name": "Churros (4 pc)", "description": "Cinnamon-sugar fried dough sticks with chocolate dipping sauce.", "category": "Desserts", "price": 6.49, "calories": 380, "protein_g": 4, "fat_g": 18, "carbs_g": 52, "allergens": ["wheat", "milk", "egg"]},
            {"name": "Horchata", "description": "Traditional rice and cinnamon drink, served cold.", "category": "Drinks", "price": 3.99, "calories": 160, "protein_g": 2, "fat_g": 2, "carbs_g": 34, "allergens": []},
            {"name": "Jarritos Soda", "description": "Imported Mexican fruit soda, assorted flavors.", "category": "Drinks", "price": 2.99, "calories": 120, "protein_g": 0, "fat_g": 0, "carbs_g": 32, "allergens": []},
        ],
    },
    {
        "brand_name": "Burrito Bandits",
        "tagline": "Burritos bigger than your head",
        "cuisine": "Mexican",
        "items": [
            {"name": "Classic Burrito", "description": "Flour tortilla stuffed with rice, beans, cheese, sour cream, and choice of protein.", "category": "Mains", "price": 12.49, "calories": 680, "protein_g": 32, "fat_g": 28, "carbs_g": 72, "allergens": ["wheat", "milk"]},
            {"name": "Carnitas Burrito", "description": "Slow-roasted pulled pork with pinto beans, salsa, and guacamole.", "category": "Mains", "price": 13.99, "calories": 720, "protein_g": 36, "fat_g": 30, "carbs_g": 70, "allergens": ["wheat", "milk"]},
            {"name": "Veggie Burrito Bowl", "description": "No tortilla - rice, black beans, roasted veggies, corn salsa, and guac.", "category": "Mains", "price": 11.49, "calories": 480, "protein_g": 16, "fat_g": 18, "carbs_g": 64, "allergens": []},
            {"name": "Chicken Nachos", "description": "Tortilla chips loaded with shredded chicken, cheese, jalapenos, and sour cream.", "category": "Appetizers", "price": 10.99, "calories": 620, "protein_g": 28, "fat_g": 34, "carbs_g": 48, "allergens": ["milk"]},
            {"name": "Black Bean Soup", "description": "Hearty black bean soup with lime, cilantro, and tortilla strips.", "category": "Appetizers", "price": 6.49, "calories": 220, "protein_g": 12, "fat_g": 4, "carbs_g": 36, "allergens": []},
            {"name": "Mexican Rice", "description": "Tomato-seasoned rice with peas and corn.", "category": "Sides", "price": 3.99, "calories": 190, "protein_g": 4, "fat_g": 4, "carbs_g": 38, "allergens": []},
            {"name": "Tres Leches Cake", "description": "Sponge cake soaked in three kinds of milk, topped with whipped cream.", "category": "Desserts", "price": 7.49, "calories": 420, "protein_g": 8, "fat_g": 16, "carbs_g": 62, "allergens": ["wheat", "milk", "egg"]},
            {"name": "Agua Fresca", "description": "Watermelon or pineapple infused water.", "category": "Drinks", "price": 3.49, "calories": 80, "protein_g": 0, "fat_g": 0, "carbs_g": 20, "allergens": []},
        ],
    },
    {
        "brand_name": "Five Gals Burgers",
        "tagline": "Handcrafted burgers, honest ingredients",
        "cuisine": "American",
        "items": [
            {"name": "Classic Cheeseburger", "description": "Angus beef patty with American cheese, lettuce, tomato, and special sauce.", "category": "Mains", "price": 11.99, "calories": 620, "protein_g": 36, "fat_g": 34, "carbs_g": 40, "allergens": ["wheat", "milk", "egg", "sesame"]},
            {"name": "Bacon BBQ Burger", "description": "Beef patty with crispy bacon, cheddar, BBQ sauce, and onion rings.", "category": "Mains", "price": 14.49, "calories": 780, "protein_g": 42, "fat_g": 42, "carbs_g": 52, "allergens": ["wheat", "milk", "egg", "soy"]},
            {"name": "Mushroom Swiss Burger", "description": "Beef patty topped with sauteed mushrooms and melted Swiss cheese.", "category": "Mains", "price": 13.49, "calories": 660, "protein_g": 38, "fat_g": 36, "carbs_g": 42, "allergens": ["wheat", "milk"]},
            {"name": "Veggie Burger", "description": "House-made black bean patty with avocado, sprouts, and chipotle mayo.", "category": "Mains", "price": 12.49, "calories": 480, "protein_g": 18, "fat_g": 22, "carbs_g": 54, "allergens": ["wheat", "egg", "soy"]},
            {"name": "Cajun Fries", "description": "Hand-cut fries seasoned with Cajun spice blend.", "category": "Sides", "price": 4.99, "calories": 380, "protein_g": 4, "fat_g": 20, "carbs_g": 46, "allergens": []},
            {"name": "Onion Rings", "description": "Beer-battered thick-cut onion rings.", "category": "Sides", "price": 5.49, "calories": 420, "protein_g": 6, "fat_g": 24, "carbs_g": 46, "allergens": ["wheat", "egg"]},
            {"name": "Milkshake", "description": "Hand-spun vanilla, chocolate, or strawberry milkshake.", "category": "Drinks", "price": 5.99, "calories": 540, "protein_g": 12, "fat_g": 22, "carbs_g": 74, "allergens": ["milk"]},
            {"name": "Brownie Sundae", "description": "Warm chocolate brownie with vanilla ice cream and hot fudge.", "category": "Desserts", "price": 7.49, "calories": 580, "protein_g": 8, "fat_g": 28, "carbs_g": 76, "allergens": ["wheat", "milk", "egg", "soy"]},
        ],
    },
    {
        "brand_name": "Cluck Yeah",
        "tagline": "Fried chicken with attitude",
        "cuisine": "American",
        "items": [
            {"name": "Original Fried Chicken (3 pc)", "description": "Buttermilk-brined, double-dredged, crispy fried chicken pieces.", "category": "Mains", "price": 12.99, "calories": 680, "protein_g": 42, "fat_g": 36, "carbs_g": 32, "allergens": ["wheat", "milk", "egg"]},
            {"name": "Nashville Hot Chicken Sandwich", "description": "Spicy cayenne-glazed chicken breast on a brioche bun with pickles and coleslaw.", "category": "Mains", "price": 13.49, "calories": 720, "protein_g": 36, "fat_g": 38, "carbs_g": 56, "allergens": ["wheat", "milk", "egg", "sesame"]},
            {"name": "Chicken Tenders (5 pc)", "description": "Hand-breaded chicken tenders with choice of dipping sauce.", "category": "Mains", "price": 10.99, "calories": 540, "protein_g": 34, "fat_g": 26, "carbs_g": 38, "allergens": ["wheat", "egg"]},
            {"name": "Chicken & Waffles", "description": "Two fried chicken pieces on a Belgian waffle with maple syrup.", "category": "Mains", "price": 14.99, "calories": 820, "protein_g": 38, "fat_g": 40, "carbs_g": 72, "allergens": ["wheat", "milk", "egg"]},
            {"name": "Mac & Cheese", "description": "Creamy baked mac and cheese with a crispy breadcrumb topping.", "category": "Sides", "price": 5.49, "calories": 420, "protein_g": 16, "fat_g": 22, "carbs_g": 40, "allergens": ["wheat", "milk", "egg"]},
            {"name": "Coleslaw", "description": "Classic creamy coleslaw with cabbage and carrot.", "category": "Sides", "price": 3.99, "calories": 180, "protein_g": 2, "fat_g": 14, "carbs_g": 12, "allergens": ["egg"]},
            {"name": "Cornbread Muffin", "description": "Sweet buttery cornbread muffin with honey butter.", "category": "Sides", "price": 2.99, "calories": 260, "protein_g": 4, "fat_g": 12, "carbs_g": 34, "allergens": ["wheat", "milk", "egg"]},
            {"name": "Sweet Tea", "description": "Southern-style sweet iced tea.", "category": "Drinks", "price": 2.49, "calories": 90, "protein_g": 0, "fat_g": 0, "carbs_g": 24, "allergens": []},
            {"name": "Peach Cobbler", "description": "Warm peach cobbler with vanilla ice cream.", "category": "Desserts", "price": 6.99, "calories": 440, "protein_g": 4, "fat_g": 18, "carbs_g": 66, "allergens": ["wheat", "milk", "egg"]},
        ],
    },
    {
        "brand_name": "Wing Commander",
        "tagline": "Mission: flavor",
        "cuisine": "American",
        "items": [
            {"name": "Buffalo Wings (10 pc)", "description": "Classic buffalo hot wings tossed in cayenne-butter sauce.", "category": "Mains", "price": 13.99, "calories": 580, "protein_g": 44, "fat_g": 38, "carbs_g": 8, "allergens": ["milk"]},
            {"name": "Garlic Parmesan Wings (10 pc)", "description": "Crispy wings in garlic butter and parmesan.", "category": "Mains", "price": 14.49, "calories": 620, "protein_g": 44, "fat_g": 42, "carbs_g": 12, "allergens": ["milk"]},
            {"name": "Korean BBQ Wings (10 pc)", "description": "Sticky-sweet gochujang-glazed wings with sesame seeds.", "category": "Mains", "price": 14.99, "calories": 640, "protein_g": 42, "fat_g": 36, "carbs_g": 28, "allergens": ["soy", "sesame", "wheat"]},
            {"name": "Lemon Pepper Wings (10 pc)", "description": "Dry-rubbed wings with zesty lemon pepper seasoning.", "category": "Mains", "price": 13.99, "calories": 540, "protein_g": 44, "fat_g": 34, "carbs_g": 6, "allergens": []},
            {"name": "Celery & Carrot Sticks", "description": "Fresh celery and carrot sticks with ranch and blue cheese.", "category": "Sides", "price": 3.99, "calories": 180, "protein_g": 4, "fat_g": 16, "carbs_g": 6, "allergens": ["milk", "egg"]},
            {"name": "Loaded Potato Skins (4 pc)", "description": "Crispy potato skins with bacon, cheddar, and sour cream.", "category": "Appetizers", "price": 8.99, "calories": 480, "protein_g": 16, "fat_g": 30, "carbs_g": 34, "allergens": ["milk"]},
            {"name": "Mozzarella Sticks (6 pc)", "description": "Breaded mozzarella sticks with marinara sauce.", "category": "Appetizers", "price": 7.99, "calories": 420, "protein_g": 18, "fat_g": 24, "carbs_g": 32, "allergens": ["wheat", "milk", "egg"]},
            {"name": "Draft Root Beer", "description": "Ice-cold root beer on draft.", "category": "Drinks", "price": 2.99, "calories": 140, "protein_g": 0, "fat_g": 0, "carbs_g": 36, "allergens": []},
        ],
    },
    {
        "brand_name": "Grain & Glory",
        "tagline": "Whole food, whole life",
        "cuisine": "Health",
        "items": [
            {"name": "Quinoa Power Bowl", "description": "Tri-color quinoa with roasted sweet potato, kale, chickpeas, and tahini dressing.", "category": "Mains", "price": 13.99, "calories": 420, "protein_g": 16, "fat_g": 18, "carbs_g": 52, "allergens": ["sesame"]},
            {"name": "Salmon Poke Bowl", "description": "Sushi-grade salmon over brown rice with avocado, edamame, and ponzu.", "category": "Mains", "price": 16.49, "calories": 480, "protein_g": 32, "fat_g": 20, "carbs_g": 44, "allergens": ["fish", "soy", "sesame"]},
            {"name": "Mediterranean Wrap", "description": "Whole-wheat wrap with grilled chicken, hummus, feta, cucumber, and tomato.", "category": "Mains", "price": 12.49, "calories": 440, "protein_g": 30, "fat_g": 16, "carbs_g": 44, "allergens": ["wheat", "milk", "sesame"]},
            {"name": "Acai Bowl", "description": "Blended acai topped with granola, banana, berries, and honey.", "category": "Mains", "price": 11.99, "calories": 380, "protein_g": 8, "fat_g": 12, "carbs_g": 62, "allergens": ["tree_nut"]},
            {"name": "Avocado Toast", "description": "Smashed avocado on sourdough with everything seasoning and microgreens.", "category": "Appetizers", "price": 9.49, "calories": 320, "protein_g": 8, "fat_g": 18, "carbs_g": 34, "allergens": ["wheat", "sesame"]},
            {"name": "Kale Caesar Salad", "description": "Massaged kale with Caesar dressing, shaved parmesan, and croutons.", "category": "Sides", "price": 8.99, "calories": 280, "protein_g": 10, "fat_g": 18, "carbs_g": 20, "allergens": ["wheat", "milk", "egg", "fish"]},
            {"name": "Cold-Pressed Green Juice", "description": "Kale, cucumber, celery, apple, ginger, and lemon.", "category": "Drinks", "price": 7.49, "calories": 120, "protein_g": 2, "fat_g": 0, "carbs_g": 28, "allergens": []},
            {"name": "Protein Smoothie", "description": "Banana, peanut butter, oat milk, and plant protein.", "category": "Drinks", "price": 8.49, "calories": 340, "protein_g": 24, "fat_g": 12, "carbs_g": 40, "allergens": ["peanut"]},
        ],
    },
    {
        "brand_name": "NootroNourish",
        "tagline": "Feed your brain",
        "cuisine": "Health",
        "items": [
            {"name": "Brain Boost Bowl", "description": "Wild salmon, blueberries, walnuts, and spinach over farro with turmeric dressing.", "category": "Mains", "price": 17.49, "calories": 520, "protein_g": 34, "fat_g": 24, "carbs_g": 42, "allergens": ["fish", "tree_nut"]},
            {"name": "Adaptogen Mushroom Wrap", "description": "Lion's mane mushrooms with hummus, roasted beets, and arugula in a flax wrap.", "category": "Mains", "price": 14.49, "calories": 380, "protein_g": 14, "fat_g": 16, "carbs_g": 46, "allergens": ["sesame"]},
            {"name": "Omega-3 Sardine Toast", "description": "Wild sardines on sourdough with lemon, capers, and pickled onion.", "category": "Appetizers", "price": 10.99, "calories": 340, "protein_g": 22, "fat_g": 14, "carbs_g": 30, "allergens": ["wheat", "fish"]},
            {"name": "Turmeric Golden Latte", "description": "Oat milk with turmeric, black pepper, cinnamon, and honey.", "category": "Drinks", "price": 5.99, "calories": 140, "protein_g": 4, "fat_g": 4, "carbs_g": 22, "allergens": []},
            {"name": "Focus Matcha Latte", "description": "Ceremonial-grade matcha whisked with almond milk.", "category": "Drinks", "price": 5.49, "calories": 120, "protein_g": 2, "fat_g": 4, "carbs_g": 18, "allergens": ["tree_nut"]},
            {"name": "Dark Chocolate Bark", "description": "85% dark chocolate with goji berries, pumpkin seeds, and sea salt.", "category": "Desserts", "price": 6.49, "calories": 280, "protein_g": 6, "fat_g": 20, "carbs_g": 24, "allergens": ["milk", "soy", "tree_nut"]},
            {"name": "Gut Health Probiotic Bowl", "description": "Kimchi, tempeh, sauerkraut, and brown rice with miso dressing.", "category": "Mains", "price": 13.99, "calories": 360, "protein_g": 18, "fat_g": 10, "carbs_g": 48, "allergens": ["soy"]},
            {"name": "Memory Trail Mix", "description": "Walnuts, almonds, dark chocolate chips, dried blueberries, and pumpkin seeds.", "category": "Sides", "price": 4.99, "calories": 320, "protein_g": 10, "fat_g": 22, "carbs_g": 24, "allergens": ["tree_nut", "milk", "soy"]},
        ],
    },
    {
        "brand_name": "Green Machine",
        "tagline": "100% plant-powered",
        "cuisine": "Health",
        "items": [
            {"name": "Beyond Burger", "description": "Plant-based patty with vegan cheese, lettuce, tomato, and special sauce.", "category": "Mains", "price": 14.49, "calories": 520, "protein_g": 24, "fat_g": 28, "carbs_g": 44, "allergens": ["wheat", "soy"]},
            {"name": "Cauliflower Tacos (3 pc)", "description": "Roasted cauliflower with cashew crema, pickled onion, and cilantro.", "category": "Mains", "price": 12.99, "calories": 380, "protein_g": 12, "fat_g": 18, "carbs_g": 44, "allergens": ["tree_nut"]},
            {"name": "Thai Coconut Soup", "description": "Creamy coconut broth with tofu, mushrooms, lemongrass, and lime.", "category": "Appetizers", "price": 8.99, "calories": 260, "protein_g": 10, "fat_g": 16, "carbs_g": 22, "allergens": ["soy"]},
            {"name": "Buddha Bowl", "description": "Brown rice, roasted chickpeas, sweet potato, avocado, and tahini.", "category": "Mains", "price": 13.49, "calories": 460, "protein_g": 14, "fat_g": 20, "carbs_g": 58, "allergens": ["sesame"]},
            {"name": "Tempeh BLT", "description": "Smoky marinated tempeh with lettuce, tomato, and vegan mayo on sourdough.", "category": "Mains", "price": 11.99, "calories": 410, "protein_g": 18, "fat_g": 20, "carbs_g": 42, "allergens": ["wheat", "soy"]},
            {"name": "Sweet Potato Fries", "description": "Crispy baked sweet potato fries with chipotle aioli.", "category": "Sides", "price": 5.49, "calories": 340, "protein_g": 4, "fat_g": 16, "carbs_g": 46, "allergens": ["soy"]},
            {"name": "Raw Cacao Smoothie", "description": "Banana, raw cacao, almond butter, oat milk, and dates.", "category": "Drinks", "price": 7.99, "calories": 380, "protein_g": 10, "fat_g": 16, "carbs_g": 52, "allergens": ["tree_nut"]},
            {"name": "Coconut Chia Pudding", "description": "Chia seeds soaked in coconut milk with mango and passion fruit.", "category": "Desserts", "price": 6.99, "calories": 280, "protein_g": 6, "fat_g": 14, "carbs_g": 34, "allergens": ["tree_nut"]},
        ],
    },
    {
        "brand_name": "Pasta La Vista",
        "tagline": "Fresh pasta, Italian soul",
        "cuisine": "Italian",
        "items": [
            {"name": "Spaghetti Bolognese", "description": "House-made spaghetti with slow-simmered beef and pork ragu.", "category": "Mains", "price": 14.99, "calories": 580, "protein_g": 28, "fat_g": 20, "carbs_g": 68, "allergens": ["wheat", "egg", "milk"]},
            {"name": "Fettuccine Alfredo", "description": "Fresh fettuccine in a rich parmesan cream sauce.", "category": "Mains", "price": 13.99, "calories": 640, "protein_g": 20, "fat_g": 32, "carbs_g": 66, "allergens": ["wheat", "milk", "egg"]},
            {"name": "Margherita Pizza", "description": "Wood-fired pizza with San Marzano tomatoes, fresh mozzarella, and basil.", "category": "Mains", "price": 13.49, "calories": 540, "protein_g": 22, "fat_g": 20, "carbs_g": 64, "allergens": ["wheat", "milk"]},
            {"name": "Penne Arrabbiata", "description": "Penne pasta in a spicy tomato sauce with garlic and red chili flakes.", "category": "Mains", "price": 12.49, "calories": 460, "protein_g": 14, "fat_g": 12, "carbs_g": 72, "allergens": ["wheat"]},
            {"name": "Bruschetta (4 pc)", "description": "Toasted ciabatta with diced tomatoes, garlic, basil, and olive oil.", "category": "Appetizers", "price": 8.49, "calories": 280, "protein_g": 6, "fat_g": 14, "carbs_g": 32, "allergens": ["wheat"]},
            {"name": "Caprese Salad", "description": "Fresh mozzarella, heirloom tomatoes, and basil with balsamic glaze.", "category": "Sides", "price": 9.99, "calories": 240, "protein_g": 14, "fat_g": 16, "carbs_g": 8, "allergens": ["milk"]},
            {"name": "Tiramisu", "description": "Classic espresso-soaked ladyfingers layered with mascarpone cream.", "category": "Desserts", "price": 8.49, "calories": 420, "protein_g": 8, "fat_g": 24, "carbs_g": 44, "allergens": ["wheat", "milk", "egg"]},
            {"name": "Italian Soda", "description": "Sparkling water with house-made fruit syrup and cream.", "category": "Drinks", "price": 3.99, "calories": 120, "protein_g": 0, "fat_g": 2, "carbs_g": 28, "allergens": ["milk"]},
        ],
    },
    {
        "brand_name": "Pizza My Heart",
        "tagline": "Every slice tells a story",
        "cuisine": "Italian",
        "items": [
            {"name": "Pepperoni Pizza", "description": "Classic pepperoni with mozzarella and house red sauce on hand-tossed dough.", "category": "Mains", "price": 14.99, "calories": 600, "protein_g": 26, "fat_g": 26, "carbs_g": 62, "allergens": ["wheat", "milk"]},
            {"name": "BBQ Chicken Pizza", "description": "Grilled chicken, red onion, cilantro, and smoky BBQ sauce.", "category": "Mains", "price": 15.99, "calories": 580, "protein_g": 30, "fat_g": 22, "carbs_g": 64, "allergens": ["wheat", "milk", "soy"]},
            {"name": "Veggie Supreme Pizza", "description": "Mushrooms, peppers, olives, onions, and artichoke hearts on white sauce.", "category": "Mains", "price": 14.49, "calories": 480, "protein_g": 18, "fat_g": 18, "carbs_g": 60, "allergens": ["wheat", "milk"]},
            {"name": "Truffle Mushroom Pizza", "description": "Wild mushroom medley with truffle oil, fontina, and arugula.", "category": "Mains", "price": 17.49, "calories": 520, "protein_g": 20, "fat_g": 24, "carbs_g": 56, "allergens": ["wheat", "milk"]},
            {"name": "Garlic Knots (6 pc)", "description": "Baked dough knots brushed with garlic butter and parsley.", "category": "Appetizers", "price": 6.49, "calories": 360, "protein_g": 8, "fat_g": 16, "carbs_g": 44, "allergens": ["wheat", "milk"]},
            {"name": "Caesar Salad", "description": "Romaine lettuce, croutons, parmesan, and creamy Caesar dressing.", "category": "Sides", "price": 7.99, "calories": 260, "protein_g": 8, "fat_g": 18, "carbs_g": 18, "allergens": ["wheat", "milk", "egg", "fish"]},
            {"name": "Cannoli (2 pc)", "description": "Crispy pastry shells filled with sweet ricotta and chocolate chips.", "category": "Desserts", "price": 7.49, "calories": 380, "protein_g": 8, "fat_g": 20, "carbs_g": 44, "allergens": ["wheat", "milk", "egg"]},
            {"name": "Espresso", "description": "Double shot of Italian espresso.", "category": "Drinks", "price": 3.49, "calories": 5, "protein_g": 0, "fat_g": 0, "carbs_g": 1, "allergens": []},
        ],
    },
    {
        "brand_name": "Seoul Food",
        "tagline": "Korean comfort, modern twist",
        "cuisine": "Korean",
        "items": [
            {"name": "Bibimbap", "description": "Hot stone bowl with rice, seasoned vegetables, beef, egg, and gochujang.", "category": "Mains", "price": 15.49, "calories": 560, "protein_g": 28, "fat_g": 18, "carbs_g": 68, "allergens": ["soy", "egg", "sesame"]},
            {"name": "Bulgogi Beef Bowl", "description": "Marinated grilled beef with pickled radish, kimchi, and steamed rice.", "category": "Mains", "price": 14.99, "calories": 520, "protein_g": 32, "fat_g": 16, "carbs_g": 58, "allergens": ["soy", "sesame"]},
            {"name": "Korean Fried Chicken (6 pc)", "description": "Double-fried chicken pieces coated in sweet-spicy gochujang glaze.", "category": "Mains", "price": 13.99, "calories": 640, "protein_g": 36, "fat_g": 32, "carbs_g": 44, "allergens": ["wheat", "soy", "sesame"]},
            {"name": "Japchae", "description": "Stir-fried glass noodles with vegetables, sesame oil, and soy sauce.", "category": "Mains", "price": 12.49, "calories": 380, "protein_g": 10, "fat_g": 12, "carbs_g": 58, "allergens": ["soy", "sesame", "wheat"]},
            {"name": "Kimchi Jjigae", "description": "Spicy kimchi stew with pork belly, tofu, and scallions.", "category": "Appetizers", "price": 9.99, "calories": 320, "protein_g": 18, "fat_g": 16, "carbs_g": 28, "allergens": ["soy", "shellfish"]},
            {"name": "Tteokbokki", "description": "Chewy rice cakes in fiery gochujang sauce with fish cake.", "category": "Sides", "price": 7.49, "calories": 340, "protein_g": 8, "fat_g": 6, "carbs_g": 64, "allergens": ["wheat", "soy", "fish"]},
            {"name": "Soju Slushie", "description": "Frozen fruit soju blend (non-alcoholic version available).", "category": "Drinks", "price": 6.99, "calories": 180, "protein_g": 0, "fat_g": 0, "carbs_g": 36, "allergens": []},
            {"name": "Hotteok", "description": "Sweet Korean pancake filled with brown sugar, cinnamon, and nuts.", "category": "Desserts", "price": 5.99, "calories": 320, "protein_g": 4, "fat_g": 10, "carbs_g": 54, "allergens": ["wheat", "tree_nut"]},
        ],
    },
    {
        "brand_name": "Curry Up",
        "tagline": "Spice is the variety of life",
        "cuisine": "Indian",
        "items": [
            {"name": "Butter Chicken", "description": "Tandoori chicken in a creamy tomato-butter sauce with basmati rice.", "category": "Mains", "price": 15.49, "calories": 560, "protein_g": 32, "fat_g": 26, "carbs_g": 48, "allergens": ["milk", "tree_nut"]},
            {"name": "Chana Masala", "description": "Spiced chickpea curry with tomatoes, onions, and cumin, served with naan.", "category": "Mains", "price": 12.99, "calories": 420, "protein_g": 14, "fat_g": 12, "carbs_g": 64, "allergens": ["wheat"]},
            {"name": "Lamb Vindaloo", "description": "Fiery Goan-style lamb curry with potatoes and vinegar.", "category": "Mains", "price": 16.99, "calories": 520, "protein_g": 30, "fat_g": 24, "carbs_g": 42, "allergens": []},
            {"name": "Palak Paneer", "description": "Cubes of paneer cheese in a creamy spinach sauce.", "category": "Mains", "price": 13.49, "calories": 380, "protein_g": 18, "fat_g": 22, "carbs_g": 26, "allergens": ["milk"]},
            {"name": "Samosas (3 pc)", "description": "Crispy pastry filled with spiced potatoes and peas, with tamarind chutney.", "category": "Appetizers", "price": 7.49, "calories": 340, "protein_g": 6, "fat_g": 18, "carbs_g": 38, "allergens": ["wheat"]},
            {"name": "Garlic Naan", "description": "Tandoor-baked flatbread brushed with garlic butter.", "category": "Sides", "price": 3.99, "calories": 260, "protein_g": 6, "fat_g": 8, "carbs_g": 42, "allergens": ["wheat", "milk"]},
            {"name": "Mango Lassi", "description": "Chilled yogurt drink blended with Alphonso mango pulp.", "category": "Drinks", "price": 4.49, "calories": 180, "protein_g": 6, "fat_g": 4, "carbs_g": 32, "allergens": ["milk"]},
            {"name": "Gulab Jamun (3 pc)", "description": "Deep-fried milk dumplings soaked in rose-cardamom syrup.", "category": "Desserts", "price": 6.49, "calories": 360, "protein_g": 4, "fat_g": 14, "carbs_g": 56, "allergens": ["wheat", "milk"]},
        ],
    },
    {
        "brand_name": "Mediterranean Nights",
        "tagline": "From the shores of the Mediterranean",
        "cuisine": "Mediterranean",
        "items": [
            {"name": "Lamb Shawarma Plate", "description": "Spit-roasted lamb with rice pilaf, hummus, pickled turnips, and tahini.", "category": "Mains", "price": 16.49, "calories": 580, "protein_g": 34, "fat_g": 26, "carbs_g": 52, "allergens": ["sesame"]},
            {"name": "Chicken Souvlaki Wrap", "description": "Grilled chicken skewers in warm pita with tzatziki, tomato, and red onion.", "category": "Mains", "price": 13.49, "calories": 460, "protein_g": 30, "fat_g": 16, "carbs_g": 46, "allergens": ["wheat", "milk"]},
            {"name": "Falafel Plate", "description": "Crispy chickpea fritters with tahini, Israeli salad, and warm pita.", "category": "Mains", "price": 12.99, "calories": 440, "protein_g": 16, "fat_g": 20, "carbs_g": 52, "allergens": ["wheat", "sesame"]},
            {"name": "Grilled Halloumi Salad", "description": "Grilled halloumi cheese over mixed greens with pomegranate and walnuts.", "category": "Mains", "price": 14.49, "calories": 380, "protein_g": 20, "fat_g": 24, "carbs_g": 22, "allergens": ["milk", "tree_nut"]},
            {"name": "Hummus & Pita", "description": "Creamy hummus drizzled with olive oil, served with warm pita wedges.", "category": "Appetizers", "price": 7.99, "calories": 280, "protein_g": 8, "fat_g": 14, "carbs_g": 32, "allergens": ["wheat", "sesame"]},
            {"name": "Baba Ganoush", "description": "Smoky roasted eggplant dip with tahini, lemon, and garlic.", "category": "Appetizers", "price": 7.49, "calories": 220, "protein_g": 4, "fat_g": 14, "carbs_g": 22, "allergens": ["sesame"]},
            {"name": "Baklava (2 pc)", "description": "Layers of phyllo dough with honey, walnuts, and pistachios.", "category": "Desserts", "price": 6.99, "calories": 340, "protein_g": 6, "fat_g": 18, "carbs_g": 42, "allergens": ["wheat", "tree_nut"]},
            {"name": "Turkish Coffee", "description": "Traditional fine-ground coffee brewed in a cezve.", "category": "Drinks", "price": 3.99, "calories": 10, "protein_g": 0, "fat_g": 0, "carbs_g": 2, "allergens": []},
            {"name": "Mint Lemonade", "description": "Fresh-squeezed lemonade with muddled mint leaves.", "category": "Drinks", "price": 3.49, "calories": 100, "protein_g": 0, "fat_g": 0, "carbs_g": 26, "allergens": []},
        ],
    },
]

# ---------------------------------------------------------------------------
# PDF Generation
# ---------------------------------------------------------------------------

class MenuPDF(FPDF):
    """Custom FPDF subclass for restaurant menu generation."""

    def __init__(self, brand_name: str, tagline: str, cuisine: str):
        super().__init__()
        self.brand_name = brand_name
        self.tagline = tagline
        self.cuisine = cuisine
        self.set_auto_page_break(auto=True, margin=25)

    def header(self):
        self.set_font("Helvetica", "B", 22)
        self.cell(0, 12, self.brand_name, new_x="LMARGIN", new_y="NEXT", align="C")
        self.set_font("Helvetica", "I", 11)
        self.cell(0, 6, self.tagline, new_x="LMARGIN", new_y="NEXT", align="C")
        self.set_font("Helvetica", "", 9)
        self.cell(0, 5, f"Cuisine: {self.cuisine}", new_x="LMARGIN", new_y="NEXT", align="C")
        self.ln(4)
        self.set_draw_color(80, 80, 80)
        self.set_line_width(0.4)
        self.line(10, self.get_y(), 200, self.get_y())
        self.ln(4)

    def footer(self):
        self.set_y(-22)
        self.set_draw_color(80, 80, 80)
        self.set_line_width(0.3)
        self.line(10, self.get_y(), 200, self.get_y())
        self.ln(2)
        self.set_font("Helvetica", "I", 7)
        allergen_text = "Allergens: " + " | ".join(
            f"[{code.upper()}] {label}" for code, label in ALLERGEN_LEGEND.items()
        )
        self.cell(0, 3, allergen_text, new_x="LMARGIN", new_y="NEXT", align="C")
        self.cell(
            0, 3,
            "Please inform your server of any food allergies. Menu items may contain or come into contact with allergens.",
            new_x="LMARGIN", new_y="NEXT", align="C",
        )
        self.cell(0, 3, f"Page {self.page_no()}", align="C")


def _allergen_tags(allergens: list[str]) -> str:
    if not allergens:
        return ""
    return "  [" + ", ".join(a.upper() for a in allergens) + "]"


def generate_pdf(brand: dict) -> str:
    """Generate a single brand menu PDF. Returns the output file path."""
    pdf = MenuPDF(brand["brand_name"], brand["tagline"], brand["cuisine"])
    pdf.add_page()

    categories_order = ["Appetizers", "Mains", "Sides", "Desserts", "Drinks"]
    items_by_cat: dict[str, list] = {}
    for item in brand["items"]:
        items_by_cat.setdefault(item["category"], []).append(item)

    for cat in categories_order:
        cat_items = items_by_cat.get(cat, [])
        if not cat_items:
            continue

        pdf.set_font("Helvetica", "B", 14)
        pdf.cell(0, 8, cat, new_x="LMARGIN", new_y="NEXT")
        pdf.set_draw_color(160, 160, 160)
        pdf.set_line_width(0.2)
        pdf.line(10, pdf.get_y(), 200, pdf.get_y())
        pdf.ln(2)

        for item in cat_items:
            # Item name and price on the same line
            pdf.set_font("Helvetica", "B", 11)
            name_w = pdf.get_string_width(item["name"]) + 4
            pdf.cell(name_w, 6, item["name"])
            pdf.set_font("Helvetica", "", 10)
            allergen_str = _allergen_tags(item["allergens"])
            if allergen_str:
                pdf.cell(0, 6, allergen_str)
            pdf.set_font("Helvetica", "B", 11)
            price_str = f"${item['price']:.2f}"
            pdf.cell(0, 6, price_str, new_x="LMARGIN", new_y="NEXT", align="R")

            # Description (wrapped)
            pdf.set_font("Helvetica", "", 9)
            desc_lines = textwrap.wrap(item["description"], width=100)
            for line in desc_lines:
                pdf.cell(0, 4, f"  {line}", new_x="LMARGIN", new_y="NEXT")

            # Nutrition line
            pdf.set_font("Helvetica", "I", 8)
            nutrition = (
                f"  Cal: {item['calories']} | "
                f"Protein: {item['protein_g']}g | "
                f"Fat: {item['fat_g']}g | "
                f"Carbs: {item['carbs_g']}g"
            )
            pdf.cell(0, 4, nutrition, new_x="LMARGIN", new_y="NEXT")
            pdf.ln(2)

    safe_name = brand["brand_name"].lower().replace(" ", "_").replace("'", "")
    out_path = PDF_DIR / f"{safe_name}_menu.pdf"
    pdf.output(str(out_path))
    return str(out_path)


def main():
    PDF_DIR.mkdir(parents=True, exist_ok=True)

    metadata = {"brands": [], "allergen_legend": ALLERGEN_LEGEND}

    for brand in BRANDS:
        pdf_path = generate_pdf(brand)
        print(f"  Generated: {pdf_path}")

        metadata["brands"].append({
            "brand_name": brand["brand_name"],
            "tagline": brand["tagline"],
            "cuisine": brand["cuisine"],
            "pdf_filename": Path(pdf_path).name,
            "item_count": len(brand["items"]),
            "items": brand["items"],
        })

    with open(METADATA_PATH, "w") as f:
        json.dump(metadata, f, indent=2)
    print(f"  Metadata: {METADATA_PATH}")

    total_items = sum(len(b["items"]) for b in BRANDS)
    print(f"\nDone! Generated {len(BRANDS)} PDFs with {total_items} total menu items.")


if __name__ == "__main__":
    main()
