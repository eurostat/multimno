import datetime
import numpy as np

BRONZE_DIARIES = [
    {
        "user_id": bytearray(b"_\xec\xebf\xff\xc8o8\xd9Rxlmily\xc2\xdb\xc29\xddN\x91\xb4g)\xd7:'\xfbW\xe9"),
        "activity_type": "stay",
        "stay_type": "home",
        "longitude": -3.7278892993927,
        "latitude": 40.48011016845703,
        "initial_timestamp": datetime.datetime(2024, 1, 1, 0, 0),
        "final_timestamp": datetime.datetime(2024, 1, 1, 10, 8, 20, 233232),
        "year": 2024,
        "month": 1,
        "day": 1,
    },
    {
        "user_id": bytearray(b"_\xec\xebf\xff\xc8o8\xd9Rxlmily\xc2\xdb\xc29\xddN\x91\xb4g)\xd7:'\xfbW\xe9"),
        "activity_type": "move",
        "stay_type": "move",
        "longitude": np.nan,
        "latitude": np.nan,
        "initial_timestamp": datetime.datetime(2024, 1, 1, 10, 8, 20, 233232),
        "final_timestamp": datetime.datetime(2024, 1, 1, 10, 30, 1, 490655),
        "year": 2024,
        "month": 1,
        "day": 1,
    },
    {
        "user_id": bytearray(b"_\xec\xebf\xff\xc8o8\xd9Rxlmily\xc2\xdb\xc29\xddN\x91\xb4g)\xd7:'\xfbW\xe9"),
        "activity_type": "stay",
        "stay_type": "work",
        "longitude": -3.738784074783325,
        "latitude": 40.44599533081055,
        "initial_timestamp": datetime.datetime(2024, 1, 1, 10, 30, 1, 490655),
        "final_timestamp": datetime.datetime(2024, 1, 1, 15, 23, 25, 285104),
        "year": 2024,
        "month": 1,
        "day": 1,
    },
    {
        "user_id": bytearray(b"_\xec\xebf\xff\xc8o8\xd9Rxlmily\xc2\xdb\xc29\xddN\x91\xb4g)\xd7:'\xfbW\xe9"),
        "activity_type": "move",
        "stay_type": "move",
        "longitude": np.nan,
        "latitude": np.nan,
        "initial_timestamp": datetime.datetime(2024, 1, 1, 15, 23, 25, 285104),
        "final_timestamp": datetime.datetime(2024, 1, 1, 15, 37, 47, 497020),
        "year": 2024,
        "month": 1,
        "day": 1,
    },
    {
        "user_id": bytearray(b"_\xec\xebf\xff\xc8o8\xd9Rxlmily\xc2\xdb\xc29\xddN\x91\xb4g)\xd7:'\xfbW\xe9"),
        "activity_type": "stay",
        "stay_type": "other",
        "longitude": -3.7288854122161865,
        "latitude": 40.46800231933594,
        "initial_timestamp": datetime.datetime(2024, 1, 1, 15, 37, 47, 497020),
        "final_timestamp": datetime.datetime(2024, 1, 1, 17, 25, 52, 294738),
        "year": 2024,
        "month": 1,
        "day": 1,
    },
    {
        "user_id": bytearray(b"_\xec\xebf\xff\xc8o8\xd9Rxlmily\xc2\xdb\xc29\xddN\x91\xb4g)\xd7:'\xfbW\xe9"),
        "activity_type": "move",
        "stay_type": "move",
        "longitude": np.nan,
        "latitude": np.nan,
        "initial_timestamp": datetime.datetime(2024, 1, 1, 17, 25, 52, 294738),
        "final_timestamp": datetime.datetime(2024, 1, 1, 17, 38, 20, 938991),
        "year": 2024,
        "month": 1,
        "day": 1,
    },
    {
        "user_id": bytearray(b"_\xec\xebf\xff\xc8o8\xd9Rxlmily\xc2\xdb\xc29\xddN\x91\xb4g)\xd7:'\xfbW\xe9"),
        "activity_type": "stay",
        "stay_type": "other",
        "longitude": -3.7499470710754395,
        "latitude": 40.45570755004883,
        "initial_timestamp": datetime.datetime(2024, 1, 1, 17, 38, 20, 938991),
        "final_timestamp": datetime.datetime(2024, 1, 1, 18, 41, 32, 456107),
        "year": 2024,
        "month": 1,
        "day": 1,
    },
    {
        "user_id": bytearray(b"_\xec\xebf\xff\xc8o8\xd9Rxlmily\xc2\xdb\xc29\xddN\x91\xb4g)\xd7:'\xfbW\xe9"),
        "activity_type": "move",
        "stay_type": "move",
        "longitude": np.nan,
        "latitude": np.nan,
        "initial_timestamp": datetime.datetime(2024, 1, 1, 18, 41, 32, 456107),
        "final_timestamp": datetime.datetime(2024, 1, 1, 18, 59, 50, 118583),
        "year": 2024,
        "month": 1,
        "day": 1,
    },
    {
        "user_id": bytearray(b"_\xec\xebf\xff\xc8o8\xd9Rxlmily\xc2\xdb\xc29\xddN\x91\xb4g)\xd7:'\xfbW\xe9"),
        "activity_type": "stay",
        "stay_type": "home",
        "longitude": -3.7278892993927,
        "latitude": 40.48011016845703,
        "initial_timestamp": datetime.datetime(2024, 1, 1, 18, 59, 50, 118583),
        "final_timestamp": datetime.datetime(2024, 1, 1, 23, 59, 59),
        "year": 2024,
        "month": 1,
        "day": 1,
    },
    {
        "user_id": bytearray(b"k\x86\xb2s\xff4\xfc\xe1\x9dk\x80N\xffZ?WG\xad\xa4\xea\xa2/\x1dI\xc0\x1eR\xdd\xb7\x87[K"),
        "activity_type": "stay",
        "stay_type": "home",
        "longitude": -3.6510417461395264,
        "latitude": 40.371856689453125,
        "initial_timestamp": datetime.datetime(2024, 1, 1, 0, 0),
        "final_timestamp": datetime.datetime(2024, 1, 1, 9, 39, 27, 771726),
        "year": 2024,
        "month": 1,
        "day": 1,
    },
    {
        "user_id": bytearray(b"k\x86\xb2s\xff4\xfc\xe1\x9dk\x80N\xffZ?WG\xad\xa4\xea\xa2/\x1dI\xc0\x1eR\xdd\xb7\x87[K"),
        "activity_type": "move",
        "stay_type": "move",
        "longitude": np.nan,
        "latitude": np.nan,
        "initial_timestamp": datetime.datetime(2024, 1, 1, 9, 39, 27, 771726),
        "final_timestamp": datetime.datetime(2024, 1, 1, 9, 54, 46, 444578),
        "year": 2024,
        "month": 1,
        "day": 1,
    },
    {
        "user_id": bytearray(b"k\x86\xb2s\xff4\xfc\xe1\x9dk\x80N\xffZ?WG\xad\xa4\xea\xa2/\x1dI\xc0\x1eR\xdd\xb7\x87[K"),
        "activity_type": "stay",
        "stay_type": "other",
        "longitude": -3.6279196739196777,
        "latitude": 40.3892936706543,
        "initial_timestamp": datetime.datetime(2024, 1, 1, 9, 54, 46, 444578),
        "final_timestamp": datetime.datetime(2024, 1, 1, 11, 3, 10, 701027),
        "year": 2024,
        "month": 1,
        "day": 1,
    },
    {
        "user_id": bytearray(b"k\x86\xb2s\xff4\xfc\xe1\x9dk\x80N\xffZ?WG\xad\xa4\xea\xa2/\x1dI\xc0\x1eR\xdd\xb7\x87[K"),
        "activity_type": "move",
        "stay_type": "move",
        "longitude": np.nan,
        "latitude": np.nan,
        "initial_timestamp": datetime.datetime(2024, 1, 1, 11, 3, 10, 701027),
        "final_timestamp": datetime.datetime(2024, 1, 1, 11, 18, 29, 373879),
        "year": 2024,
        "month": 1,
        "day": 1,
    },
    {
        "user_id": bytearray(b"k\x86\xb2s\xff4\xfc\xe1\x9dk\x80N\xffZ?WG\xad\xa4\xea\xa2/\x1dI\xc0\x1eR\xdd\xb7\x87[K"),
        "activity_type": "stay",
        "stay_type": "home",
        "longitude": -3.6510417461395264,
        "latitude": 40.371856689453125,
        "initial_timestamp": datetime.datetime(2024, 1, 1, 11, 18, 29, 373879),
        "final_timestamp": datetime.datetime(2024, 1, 1, 23, 59, 59),
        "year": 2024,
        "month": 1,
        "day": 1,
    },
    {
        "user_id": bytearray(
            b"\xd4s^:&^\x16\xee\xe0?Yq\x8b\x9b]\x03\x01\x9c\x07\xd8\xb6\xc5\x1f\x90\xda:fn\xec\x13\xab5"
        ),
        "activity_type": "stay",
        "stay_type": "home",
        "longitude": -3.7394583225250244,
        "latitude": 40.45436096191406,
        "initial_timestamp": datetime.datetime(2024, 1, 1, 0, 0),
        "final_timestamp": datetime.datetime(2024, 1, 1, 10, 56, 28, 277559),
        "year": 2024,
        "month": 1,
        "day": 1,
    },
    {
        "user_id": bytearray(
            b"\xd4s^:&^\x16\xee\xe0?Yq\x8b\x9b]\x03\x01\x9c\x07\xd8\xb6\xc5\x1f\x90\xda:fn\xec\x13\xab5"
        ),
        "activity_type": "move",
        "stay_type": "move",
        "longitude": np.nan,
        "latitude": np.nan,
        "initial_timestamp": datetime.datetime(2024, 1, 1, 10, 56, 28, 277559),
        "final_timestamp": datetime.datetime(2024, 1, 1, 11, 6, 51, 222106),
        "year": 2024,
        "month": 1,
        "day": 1,
    },
    {
        "user_id": bytearray(
            b"\xd4s^:&^\x16\xee\xe0?Yq\x8b\x9b]\x03\x01\x9c\x07\xd8\xb6\xc5\x1f\x90\xda:fn\xec\x13\xab5"
        ),
        "activity_type": "stay",
        "stay_type": "other",
        "longitude": -3.739649534225464,
        "latitude": 40.43755340576172,
        "initial_timestamp": datetime.datetime(2024, 1, 1, 11, 6, 51, 222106),
        "final_timestamp": datetime.datetime(2024, 1, 1, 12, 15, 13, 725708),
        "year": 2024,
        "month": 1,
        "day": 1,
    },
    {
        "user_id": bytearray(
            b"\xd4s^:&^\x16\xee\xe0?Yq\x8b\x9b]\x03\x01\x9c\x07\xd8\xb6\xc5\x1f\x90\xda:fn\xec\x13\xab5"
        ),
        "activity_type": "move",
        "stay_type": "move",
        "longitude": np.nan,
        "latitude": np.nan,
        "initial_timestamp": datetime.datetime(2024, 1, 1, 12, 15, 13, 725708),
        "final_timestamp": datetime.datetime(2024, 1, 1, 12, 25, 36, 670255),
        "year": 2024,
        "month": 1,
        "day": 1,
    },
    {
        "user_id": bytearray(
            b"\xd4s^:&^\x16\xee\xe0?Yq\x8b\x9b]\x03\x01\x9c\x07\xd8\xb6\xc5\x1f\x90\xda:fn\xec\x13\xab5"
        ),
        "activity_type": "stay",
        "stay_type": "home",
        "longitude": -3.7394583225250244,
        "latitude": 40.45436096191406,
        "initial_timestamp": datetime.datetime(2024, 1, 1, 12, 25, 36, 670255),
        "final_timestamp": datetime.datetime(2024, 1, 1, 23, 59, 59),
        "year": 2024,
        "month": 1,
        "day": 1,
    },
    {
        "user_id": bytearray(b"_\xec\xebf\xff\xc8o8\xd9Rxlmily\xc2\xdb\xc29\xddN\x91\xb4g)\xd7:'\xfbW\xe9"),
        "activity_type": "stay",
        "stay_type": "home",
        "longitude": -3.7278892993927,
        "latitude": 40.48011016845703,
        "initial_timestamp": datetime.datetime(2024, 1, 2, 0, 0),
        "final_timestamp": datetime.datetime(2024, 1, 2, 11, 43, 9, 913101),
        "year": 2024,
        "month": 1,
        "day": 2,
    },
    {
        "user_id": bytearray(b"_\xec\xebf\xff\xc8o8\xd9Rxlmily\xc2\xdb\xc29\xddN\x91\xb4g)\xd7:'\xfbW\xe9"),
        "activity_type": "move",
        "stay_type": "move",
        "longitude": np.nan,
        "latitude": np.nan,
        "initial_timestamp": datetime.datetime(2024, 1, 2, 11, 43, 9, 913101),
        "final_timestamp": datetime.datetime(2024, 1, 2, 11, 55, 38, 557373),
        "year": 2024,
        "month": 1,
        "day": 2,
    },
    {
        "user_id": bytearray(b"_\xec\xebf\xff\xc8o8\xd9Rxlmily\xc2\xdb\xc29\xddN\x91\xb4g)\xd7:'\xfbW\xe9"),
        "activity_type": "stay",
        "stay_type": "other",
        "longitude": -3.7489547729492188,
        "latitude": 40.467811584472656,
        "initial_timestamp": datetime.datetime(2024, 1, 2, 11, 55, 38, 557373),
        "final_timestamp": datetime.datetime(2024, 1, 2, 14, 0, 3, 3759),
        "year": 2024,
        "month": 1,
        "day": 2,
    },
    {
        "user_id": bytearray(b"_\xec\xebf\xff\xc8o8\xd9Rxlmily\xc2\xdb\xc29\xddN\x91\xb4g)\xd7:'\xfbW\xe9"),
        "activity_type": "move",
        "stay_type": "move",
        "longitude": np.nan,
        "latitude": np.nan,
        "initial_timestamp": datetime.datetime(2024, 1, 2, 14, 0, 3, 3759),
        "final_timestamp": datetime.datetime(2024, 1, 2, 14, 12, 31, 648031),
        "year": 2024,
        "month": 1,
        "day": 2,
    },
    {
        "user_id": bytearray(b"_\xec\xebf\xff\xc8o8\xd9Rxlmily\xc2\xdb\xc29\xddN\x91\xb4g)\xd7:'\xfbW\xe9"),
        "activity_type": "stay",
        "stay_type": "home",
        "longitude": -3.7278892993927,
        "latitude": 40.48011016845703,
        "initial_timestamp": datetime.datetime(2024, 1, 2, 14, 12, 31, 648031),
        "final_timestamp": datetime.datetime(2024, 1, 2, 23, 59, 59),
        "year": 2024,
        "month": 1,
        "day": 2,
    },
    {
        "user_id": bytearray(b"k\x86\xb2s\xff4\xfc\xe1\x9dk\x80N\xffZ?WG\xad\xa4\xea\xa2/\x1dI\xc0\x1eR\xdd\xb7\x87[K"),
        "activity_type": "stay",
        "stay_type": "home",
        "longitude": -3.6510417461395264,
        "latitude": 40.371856689453125,
        "initial_timestamp": datetime.datetime(2024, 1, 2, 0, 0),
        "final_timestamp": datetime.datetime(2024, 1, 2, 5, 43, 21, 972891),
        "year": 2024,
        "month": 1,
        "day": 2,
    },
    {
        "user_id": bytearray(b"k\x86\xb2s\xff4\xfc\xe1\x9dk\x80N\xffZ?WG\xad\xa4\xea\xa2/\x1dI\xc0\x1eR\xdd\xb7\x87[K"),
        "activity_type": "move",
        "stay_type": "move",
        "longitude": np.nan,
        "latitude": np.nan,
        "initial_timestamp": datetime.datetime(2024, 1, 2, 5, 43, 21, 972891),
        "final_timestamp": datetime.datetime(2024, 1, 2, 6, 0, 16, 678032),
        "year": 2024,
        "month": 1,
        "day": 2,
    },
    {
        "user_id": bytearray(b"k\x86\xb2s\xff4\xfc\xe1\x9dk\x80N\xffZ?WG\xad\xa4\xea\xa2/\x1dI\xc0\x1eR\xdd\xb7\x87[K"),
        "activity_type": "stay",
        "stay_type": "work",
        "longitude": -3.6409952640533447,
        "latitude": 40.39813995361328,
        "initial_timestamp": datetime.datetime(2024, 1, 2, 6, 0, 16, 678032),
        "final_timestamp": datetime.datetime(2024, 1, 2, 13, 33, 50, 606073),
        "year": 2024,
        "month": 1,
        "day": 2,
    },
    {
        "user_id": bytearray(b"k\x86\xb2s\xff4\xfc\xe1\x9dk\x80N\xffZ?WG\xad\xa4\xea\xa2/\x1dI\xc0\x1eR\xdd\xb7\x87[K"),
        "activity_type": "move",
        "stay_type": "move",
        "longitude": np.nan,
        "latitude": np.nan,
        "initial_timestamp": datetime.datetime(2024, 1, 2, 13, 33, 50, 606073),
        "final_timestamp": datetime.datetime(2024, 1, 2, 13, 49, 9, 278869),
        "year": 2024,
        "month": 1,
        "day": 2,
    },
    {
        "user_id": bytearray(b"k\x86\xb2s\xff4\xfc\xe1\x9dk\x80N\xffZ?WG\xad\xa4\xea\xa2/\x1dI\xc0\x1eR\xdd\xb7\x87[K"),
        "activity_type": "stay",
        "stay_type": "other",
        "longitude": -3.617863893508911,
        "latitude": 40.41557693481445,
        "initial_timestamp": datetime.datetime(2024, 1, 2, 13, 49, 9, 278869),
        "final_timestamp": datetime.datetime(2024, 1, 2, 15, 8, 49, 280454),
        "year": 2024,
        "month": 1,
        "day": 2,
    },
    {
        "user_id": bytearray(b"k\x86\xb2s\xff4\xfc\xe1\x9dk\x80N\xffZ?WG\xad\xa4\xea\xa2/\x1dI\xc0\x1eR\xdd\xb7\x87[K"),
        "activity_type": "move",
        "stay_type": "move",
        "longitude": np.nan,
        "latitude": np.nan,
        "initial_timestamp": datetime.datetime(2024, 1, 2, 15, 8, 49, 280454),
        "final_timestamp": datetime.datetime(2024, 1, 2, 15, 40, 1, 45514),
        "year": 2024,
        "month": 1,
        "day": 2,
    },
    {
        "user_id": bytearray(b"k\x86\xb2s\xff4\xfc\xe1\x9dk\x80N\xffZ?WG\xad\xa4\xea\xa2/\x1dI\xc0\x1eR\xdd\xb7\x87[K"),
        "activity_type": "stay",
        "stay_type": "home",
        "longitude": -3.6510417461395264,
        "latitude": 40.371856689453125,
        "initial_timestamp": datetime.datetime(2024, 1, 2, 15, 40, 1, 45514),
        "final_timestamp": datetime.datetime(2024, 1, 2, 23, 59, 59),
        "year": 2024,
        "month": 1,
        "day": 2,
    },
    {
        "user_id": bytearray(
            b"\xd4s^:&^\x16\xee\xe0?Yq\x8b\x9b]\x03\x01\x9c\x07\xd8\xb6\xc5\x1f\x90\xda:fn\xec\x13\xab5"
        ),
        "activity_type": "stay",
        "stay_type": "home",
        "longitude": -3.7394583225250244,
        "latitude": 40.45436096191406,
        "initial_timestamp": datetime.datetime(2024, 1, 2, 0, 0),
        "final_timestamp": datetime.datetime(2024, 1, 2, 10, 13, 1, 366308),
        "year": 2024,
        "month": 1,
        "day": 2,
    },
    {
        "user_id": bytearray(
            b"\xd4s^:&^\x16\xee\xe0?Yq\x8b\x9b]\x03\x01\x9c\x07\xd8\xb6\xc5\x1f\x90\xda:fn\xec\x13\xab5"
        ),
        "activity_type": "move",
        "stay_type": "move",
        "longitude": np.nan,
        "latitude": np.nan,
        "initial_timestamp": datetime.datetime(2024, 1, 2, 10, 13, 1, 366308),
        "final_timestamp": datetime.datetime(2024, 1, 2, 10, 28, 10, 30821),
        "year": 2024,
        "month": 1,
        "day": 2,
    },
    {
        "user_id": bytearray(
            b"\xd4s^:&^\x16\xee\xe0?Yq\x8b\x9b]\x03\x01\x9c\x07\xd8\xb6\xc5\x1f\x90\xda:fn\xec\x13\xab5"
        ),
        "activity_type": "stay",
        "stay_type": "work",
        "longitude": -3.726670503616333,
        "latitude": 40.476863861083984,
        "initial_timestamp": datetime.datetime(2024, 1, 2, 10, 28, 10, 30821),
        "final_timestamp": datetime.datetime(2024, 1, 2, 14, 48, 34, 686026),
        "year": 2024,
        "month": 1,
        "day": 2,
    },
    {
        "user_id": bytearray(
            b"\xd4s^:&^\x16\xee\xe0?Yq\x8b\x9b]\x03\x01\x9c\x07\xd8\xb6\xc5\x1f\x90\xda:fn\xec\x13\xab5"
        ),
        "activity_type": "move",
        "stay_type": "move",
        "longitude": np.nan,
        "latitude": np.nan,
        "initial_timestamp": datetime.datetime(2024, 1, 2, 14, 48, 34, 686026),
        "final_timestamp": datetime.datetime(2024, 1, 2, 14, 58, 57, 630573),
        "year": 2024,
        "month": 1,
        "day": 2,
    },
    {
        "user_id": bytearray(
            b"\xd4s^:&^\x16\xee\xe0?Yq\x8b\x9b]\x03\x01\x9c\x07\xd8\xb6\xc5\x1f\x90\xda:fn\xec\x13\xab5"
        ),
        "activity_type": "stay",
        "stay_type": "other",
        "longitude": -3.7268617153167725,
        "latitude": 40.46005630493164,
        "initial_timestamp": datetime.datetime(2024, 1, 2, 14, 58, 57, 630573),
        "final_timestamp": datetime.datetime(2024, 1, 2, 16, 25, 21, 965751),
        "year": 2024,
        "month": 1,
        "day": 2,
    },
    {
        "user_id": bytearray(
            b"\xd4s^:&^\x16\xee\xe0?Yq\x8b\x9b]\x03\x01\x9c\x07\xd8\xb6\xc5\x1f\x90\xda:fn\xec\x13\xab5"
        ),
        "activity_type": "move",
        "stay_type": "move",
        "longitude": np.nan,
        "latitude": np.nan,
        "initial_timestamp": datetime.datetime(2024, 1, 2, 16, 25, 21, 965751),
        "final_timestamp": datetime.datetime(2024, 1, 2, 16, 32, 15, 226461),
        "year": 2024,
        "month": 1,
        "day": 2,
    },
    {
        "user_id": bytearray(
            b"\xd4s^:&^\x16\xee\xe0?Yq\x8b\x9b]\x03\x01\x9c\x07\xd8\xb6\xc5\x1f\x90\xda:fn\xec\x13\xab5"
        ),
        "activity_type": "stay",
        "stay_type": "home",
        "longitude": -3.7394583225250244,
        "latitude": 40.45436096191406,
        "initial_timestamp": datetime.datetime(2024, 1, 2, 16, 32, 15, 226461),
        "final_timestamp": datetime.datetime(2024, 1, 2, 23, 59, 59),
        "year": 2024,
        "month": 1,
        "day": 2,
    },
]


# [row.asDict() for row in synthetic_diaries.output_data_objects[BronzeSyntheticDiariesDataObject.ID].df.collect()]
