import os, sys, json, traceback
# Ensure project root is on sys.path so `quant_system` package can be imported when running this script
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

try:
    from quant_system import scheduler as sched_module
    sc = sched_module.scheduler
except Exception as e:
    print("IMPORT_ERROR", e)
    traceback.print_exc()
    sys.exit(1)

print('SCHEDULER_OBJ', type(sc))

try:
    tasks = sc.get_custom_tasks()
    print('CUSTOM_TASKS_COUNT', len(tasks))
    print(json.dumps(tasks, ensure_ascii=False, indent=2))
except Exception as e:
    print('GET_TASKS_ERROR', e)
    traceback.print_exc()
    sys.exit(1)

if not tasks:
    print('NO_TASKS')
    sys.exit(0)

# prefer to remove known id if present
target_id = None
for t in tasks:
    if t.get('id') == '21955533':
        target_id = t.get('id')
        break
if not target_id:
    target_id = tasks[0].get('id')

print('TARGET_ID', target_id)

try:
    ok = sc.remove_custom_task(target_id)
    print('REMOVE_OK', ok)
except Exception as e:
    print('REMOVE_ERROR', e)
    traceback.print_exc()

try:
    after = sc.get_custom_tasks()
    print('AFTER_COUNT', len(after))
    print(json.dumps(after, ensure_ascii=False, indent=2))
except Exception as e:
    print('AFTER_GET_ERROR', e)
    traceback.print_exc()
