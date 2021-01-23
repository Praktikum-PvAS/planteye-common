from common import utc_to_timestamp
from event_logger import log_event


class BufferEntity:
    """
    Buffer entity class consists of opc ua node information as well as read opcua invariant
    """

    def __init__(self, entity_type, data):
        self.entity_type = entity_type
        self.data = data

    def convert_to_line_protocol(self):
        """
        This function converts received data into influxdb line protocol
        :return:
        """
        if self.entity_type == 'opcua':
            return self._convert_opcua_data_to_line_protocol()
        elif self.entity_type == 'frame':
            return self._convert_frame_data_to_line_protocol()
        else:
            return [False, '']

    def _convert_opcua_data_to_line_protocol(self):
        """
        This function converts data received from Nebula into influxdb line protocol
        :return: status code and data line protocol
        """
        node_data = self.data['node']
        data_variant = self.data['data_variant']

        value = data_variant.Value.Value
        datatype = data_variant.Value.VariantType.name
        timestamp = utc_to_timestamp(data_variant.SourceTimestamp)
        status_code = data_variant.StatusCode.name

        if not status_code == 'Good':
            return [False, '']

        try:
            data_line = ''
            data_line += node_data.meas
            data_line += ',tag=' + node_data.tag
            data_line += ',var=' + node_data.var
            data_line += ',datatype=' + datatype
            data_line += ',statuscode=' + data_variant.StatusCode.name
            data_line += ' '

            if datatype == 'String':
                data_line += 'str_value="' + str(value) + '" ' + str(timestamp)
            elif datatype == 'Boolean':
                data_line = data_line + 'num_value=' + str(int(value)) + ' ' + str(timestamp)
            elif datatype in ['Double', 'SByte', 'Byte', 'Int16', 'UInt16', 'Int32', 'UInt32', 'Int64', 'UInt64',
                              'Float']:
                data_line = data_line + 'num_value=' + str(value) + ' ' + str(timestamp)
            else:
                data_line = data_line + 'str_value=' + str(value) + ' ' + str(timestamp)

            return [True, data_line]

        except Exception:
            return [False, '']

    def _convert_frame_data_to_line_protocol(self):
        """
        This function converts frame received from Vision into influxdb line protocol
        :return: status code and data line protocol
        """
        try:
            # Measurement
            data_line = self.data['measurement']

            # Tags
            for key, value in self.data['tags'].items():
                if isinstance(value, (int, float)):
                    data_line += ',' + key + '=' + str(value)
                else:
                    value = value.replace(' ', '')
                    data_line += ',' + key + '=' + value

            # Backspace between tags and fields
            data_line += ' '

            # Fields
            for key, value in self.data['fields'].items():
                if value.isnumeric():
                    data_line += key + '=' + str(value) + ','
                else:
                    value = value.replace(' ', '')
                    data_line += key + '="' + value + '",'

            # Removing last comma
            data_line = data_line[:-1]

            # Adding timestamp
            data_line += ' ' + str(self.data['timestamp'])

            return [True, data_line]

        except Exception:
            return [False, '']


class Buffer:
    """
    Buffer class plays role a temporary FIFO storage for data points before ingestion into influx db
    """

    def __init__(self, cfg):
        """
        Initialisation
        :param cfg: Set of parameters including maximal buffer size and text output parameters
        """
        self.module_name = 'Buffer'
        self.cfg = cfg
        self.max_buffer_size = self.cfg['buffer']['max_size']
        self.buffer = []

    def add_point(self, buffer_entity):
        """
        This function puts additional entity into buffer
        :param buffer_entity: buffer entity consisted of node instance and opcua variant
        :return:
        """

        # If after adding a point, the buffer will be overfilled, we will remove the first entity in the buffer
        if len(self.buffer) + 1 > self.max_buffer_size:
            self.remove_point(0)
            log_event(self.cfg, self.module_name, '', 'WARN', 'Buffer is full (' + str(len(self.buffer)) + ')')

        # Append entity
        self.buffer.append(buffer_entity)
        log_event(self.cfg, self.module_name, '', 'INFO', 'Point copied into buffer (size=' + str(self.len()) + ')')

    def remove_point(self, idx=0):
        """
        This function removes specified element of the buffer. If not specified, removes the very first element.
        :param idx: Index of the element to remove
        :return:
        """

        # Removing operation is only valid if valid index is provided
        if (idx < 0) or (idx > len(self.buffer) - 1):
            log_event(self.cfg, self.module_name, '', 'WARN', str(idx) + ' element does not exist in buffer')
            return
        del self.buffer[idx]
        log_event(self.cfg, self.module_name, '', 'INFO',
                  'Point ' + str(idx) + ' removed from buffer (size=' + str(self.len()) + ')')

    def remove_points(self, idx):
        """
        This function removes a set of elements.
        :param idx: list of indices
        :return:
        """
        # Removing duplicates
        idx = list(dict.fromkeys(idx))

        # Loop within sorted and reversed list
        for i in sorted(idx, reverse=True):
            self.remove_point(i)
        log_event(self.cfg, self.module_name, '', 'INFO',
                  str(len(idx)) + ' points removed from buffer (size=' + str(self.len()) + ')')

    def len(self):
        """
        This function returns the actual length of the buffer
        :return: Actual length of the buffer
        """
        return len(self.buffer)

    def get_snapshot(self):
        """
        This function creates a snapshot of the buffer in order to decouple data with the mutable list object
        :return:
        """
        return list(self.buffer)
