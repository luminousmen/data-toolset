from pathlib import Path

TEST_DATA_DIR = Path(__file__).resolve().parent

DATA_JSON_EXPECTED = [
    {
        "age": 10,
        "appearance": {
            "color": "blue",
            "size": "small"
        },
        "character": "Alice",
        "friends": [
            "Rabbit",
            "Cheshire Cat"
        ],
        "height": 150.5,
        "is_human": True,
        "quote": "Curiouser and curiouser!"
    },
    {
        "age": 35,
        "appearance": {
            "color": "green",
            "size": "tall"
        },
        "character": "Mad Hatter",
        "friends": [
            "Alice"
        ],
        "height": 175.2,
        "is_human": True,
        "quote": "I'm late!"
    },
    {
        "age": 50,
        "appearance": {
            "color": "red",
            "size": "average"
        },
        "character": "Queen of Hearts",
        "friends": [
            "White Rabbit",
            "King of Hearts"
        ],
        "height": 165.8,
        "is_human": False,
        "quote": "Off with their heads!"
    }
]

DATA_CSV_EXPECTED = [
    {'character': 'Alice', 'age': '10', 'is_human': 'True', 'height': '150.5', 'quote': 'Curiouser and curiouser!',
     'friends': "['Rabbit', 'Cheshire Cat']", 'appearance': "{'color': 'blue', 'size': 'small'}"},
    {'character': 'Mad Hatter', 'age': '35', 'is_human': 'True', 'height': '175.2', 'quote': "I'm late!",
     'friends': "['Alice']", 'appearance': "{'color': 'green', 'size': 'tall'}"},
    {'character': 'Queen of Hearts', 'age': '50', 'is_human': 'False', 'height': '165.8',
     'quote': 'Off with their heads!', 'friends': "['White Rabbit', 'King of Hearts']",
     'appearance': "{'color': 'red', 'size': 'average'}"}]
