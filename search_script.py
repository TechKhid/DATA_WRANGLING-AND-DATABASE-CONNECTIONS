from data_fr import db, UnifiedRecord
from pony.orm import db_session, select, commit


@db_session
def name_search(name_1:str, name_2:str):
    result = select(p for p in UnifiedRecord
                    if p.first_name == name_1 and p.second_name == name_2).first()
    contingency = select(p for p in UnifiedRecord
                    if p.second_name == name_2).first()
    if result is not None:
        if result:
            print(f"{name_1} {name_2} is found.")
        else:
            print(f"{name_1} {name_2} is not found X!!")
    else:
        print("Check Spelling!")
        if contingency:
            print(f"{name_2} is found.")
        else:
            print(f"{name_2} is still not found X!!")

@db_session
def salary_increase(name_1:str, name_2:str, diff:int):
    result = select(p for p in UnifiedRecord
                    if p.first_name == name_1 and p.second_name == name_2).first()
    contingency = select(p for p in UnifiedRecord
                    if p.second_name == name_2).first()
    if result is not None:
        if result:
            print(f"{name_1} {name_2} is found.")
        else:
            print(f"{name_1} {name_2} is not found X!!")
    else:
        print("Check Spelling!")
        if contingency:
            print(f"{name_2} is found.")
            contingency.salary = 0  if contingency.salary is None else contingency.salary
            prev_ = contingency.salary
            contingency.salary = prev_ + diff
            print(f"salary increased from {prev_} to {contingency.salary}!")
        else:
            print(f"{name_2} is still not found X!!")


@db_session
def update_cvv(name_1:str, name_2:str, cvv:str):
    result = select(p for p in UnifiedRecord
                    if p.first_name == name_1 and p.second_name == name_2).first()
    if result is not None:
        if result:
            print(f"{name_1} {name_2} is found.")
            prev_cvv = result.credit_card_security_code
            result.credit_card_security_code = cvv
            commit()
            print(f"cvv changed from {prev_cvv} to {cvv}!")

        else:
            print(f"{name_1} {name_2} is not found X!!")

@db_session
def update_age(name_1:str, name_2:str, age:int):
    result = select(p for p in UnifiedRecord
                    if p.first_name == name_1 and p.second_name == name_2).first()
    if result is not None:
        if result:
            print(f"{name_1} {name_2} is found.")
            prev_age = result.age
            result.age = age
            commit()
            print(f"age changed from {prev_age} to {age}!")

        else:
            print(f"{name_1} {name_2} is not found X!!")
    else:
        print("Check Spelling!")

if __name__ == "__main__":
#    update_cvv("Valerie", "Ellis", '762')
#    update_age("Charlie", "Short", 52)
    salary_increase(" ", "West", 2100)


    