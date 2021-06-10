from relion.dbmodel import modeltables
from relion.protonode.protonode import ProtoNode


class DBNode(ProtoNode):
    def __init__(self, name, tables, **kwargs):
        super().__init__(name, **kwargs)
        self.tables = tables
        for table in self.tables:
            table._last_update[self.name] = 0
        self._sent = [[] for _ in self.tables]
        self._unsent = [[] for _ in self.tables]

    def __eq__(self, other):
        if isinstance(other, DBNode):
            if self.name == other.name and len(self._out) == len(other._out):
                for n in self._out:
                    if n not in other._out:
                        return False
                return True
        return False

    def __hash__(self):
        return hash(("relion.dbmodel.dbnode.DBNode", self._name))

    def __repr__(self):
        return f"Node({repr(str(self._name))})"

    def __bool__(self):
        if self.tables:
            return True
        return False

    def __call__(self, **kwargs):
        extra_options = kwargs.get("extra_options")
        end_time = kwargs.get("end_time")
        msg_con = kwargs.get("message_constructor")
        newkwargs = {
            x: kwargs[x]
            for x in kwargs.keys()
            if x != "extra_options" and x != "end_time" and x != "message_constructor"
        }
        self.insert(end_time, extra_options, **newkwargs)
        super().__call__()
        return self.message(msg_con)

    def update_times(self, source=None):
        if source is None:
            all_times = []
            for tab in self.tables:
                for k in tab._last_update.keys():
                    all_times.append(tab._last_update[k])
        return [tab._last_update[source] for tab in self.tables]

    def insert(self, end_time, extra_options, **kwargs):
        source_option = kwargs.get("source")
        if source_option is not None:
            kwargs = {x: kwargs[x] for x in kwargs.keys() if x != "source"}
        for i, tab in enumerate(self.tables):
            kwargs = self._do_check(kwargs)

            pid = modeltables.insert(
                tab, end_time, source_option or self.name, extra_options, **kwargs
            )
            if pid is not None:
                self._unsent[i].append(pid)

    def _do_check(self, in_values):
        try:
            if in_values.get(self.environment["check_for"]) is not None:
                table = self.environment["foreign_table"]
                check_for_foreign_name = self.environment.get("check_for_foreign_name")
                if check_for_foreign_name is None:
                    check_for_foreign_name = self.environment["check_for"]
                indices = table.get_row_index(
                    check_for_foreign_name,
                    in_values.get(self.environment["check_for"]),
                )
                if indices is None:
                    return in_values
                try:
                    in_values[self.environment["foreign_key"]] = [
                        table[table._primary_key][ci] for ci in indices
                    ]
                    return in_values
                except TypeError:
                    in_values[self.environment["foreign_key"]] = table[
                        table._primary_key
                    ][indices]
                    return in_values
            else:
                return in_values
        except KeyError:
            return in_values

    def message(self, constructor=None):
        if constructor is None:
            return []
        messages = []
        for tab_index, ids in enumerate(self._unsent):
            for pid in ids:
                message = constructor(
                    self.tables[tab_index],
                    pid,
                )
                messages.append(message)
                self._unsent[tab_index].remove(pid)
                self._sent[tab_index].append(pid)
        return messages
