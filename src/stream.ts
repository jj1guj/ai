import { bindThis } from '@/decorators.js';
import { EventEmitter } from 'events';
import WebSocket from 'ws';
import _ReconnectingWebsocket from 'reconnecting-websocket';
import config from './config.js';
import log from './utils/log.js';

const ReconnectingWebsocket = _ReconnectingWebsocket as unknown as typeof _ReconnectingWebsocket['default'];

/**
 * Misskey stream connection
 */
export default class Stream extends EventEmitter {
	private stream: any;
	private socket?: WebSocket;
	private state: string;
	private buffer: any[];
	private awaitingPong = false;
	private heartbeatTimer: NodeJS.Timeout;
	private sharedConnectionPools: Pool[] = [];
	private sharedConnections: SharedConnection[] = [];
	private nonSharedConnections: NonSharedConnection[] = [];

	constructor() {
		super();

		this.state = 'initializing';
		this.buffer = [];
		this.openStream();
		this.heartbeatTimer = setInterval(this.checkHeartbeat, 60_000);
	}

	@bindThis
	private openStream() {
		const stream = this;
		class HeartbeatWebSocket extends WebSocket {
			constructor(url: string | URL, protocols?: string | string[]) {
				super(url, protocols);
				stream.socket = this;
				this.on('pong', stream.onPong);
			}
		}

		this.stream = new ReconnectingWebsocket(`${config.wsUrl}/streaming?i=${config.i}`, [], {
			WebSocket: HeartbeatWebSocket
		});
		this.stream.addEventListener('open', this.onOpen);
		this.stream.addEventListener('close', this.onClose);
		this.stream.addEventListener('message', this.onMessage);
	}

	@bindThis
	private resetStream() {
		this.stream.removeEventListener('open', this.onOpen);
		this.stream.removeEventListener('close', this.onClose);
		this.stream.removeEventListener('message', this.onMessage);
		this.stream.close();
		this.socket = undefined;
		this.awaitingPong = false;
		this.state = 'reconnecting';
		this.openStream();
	}

	@bindThis
	private onPong() {
		this.awaitingPong = false;
	}

	@bindThis
	private checkHeartbeat() {
		if (this.socket?.readyState === WebSocket.OPEN) {
			if (this.awaitingPong) {
				log('[Stream]: WebSocket heartbeat timed out; reconnecting...');
				this.socket.terminate();
				return;
			}

			this.awaitingPong = true;
			this.socket.ping();
			return;
		}

		log('[Stream]: WebSocket is not connected; rebuilding connection...');
		this.resetStream();
	}

	@bindThis
	public useSharedConnection(channel: string): SharedConnection {
		let pool = this.sharedConnectionPools.find(p => p.channel === channel);

		if (pool == null) {
			pool = new Pool(this, channel);
			this.sharedConnectionPools.push(pool);
		}

		const connection = new SharedConnection(this, channel, pool);
		this.sharedConnections.push(connection);
		return connection;
	}

	@bindThis
	public removeSharedConnection(connection: SharedConnection) {
		this.sharedConnections = this.sharedConnections.filter(c => c !== connection);
	}

	@bindThis
	public connectToChannel(channel: string, params?: any): NonSharedConnection {
		const connection = new NonSharedConnection(this, channel, params);
		this.nonSharedConnections.push(connection);
		return connection;
	}

	@bindThis
	public disconnectToChannel(connection: NonSharedConnection) {
		this.nonSharedConnections = this.nonSharedConnections.filter(c => c !== connection);
	}

	/**
	 * Callback of when open connection
	 */
	@bindThis
	private onOpen() {
		const isReconnect = this.state == 'reconnecting';

		this.state = 'connected';
		this.awaitingPong = false;
		log(`[Stream]: WebSocket connected${isReconnect ? ' again' : ''}`);
		this.emit('_connected_');

		// バッファーを処理
		const _buffer = [...this.buffer]; // Shallow copy
		this.buffer = []; // Clear buffer
		for (const data of _buffer) {
			this.send(data); // Resend each buffered messages
		}

		// チャンネル再接続
		if (isReconnect) {
			this.sharedConnectionPools.forEach(p => {
				p.connect();
			});
			this.nonSharedConnections.forEach(c => {
				c.connect();
			});
		}
	}

	/**
	 * Callback of when close connection
	 */
	@bindThis
	private onClose() {
		this.state = 'reconnecting';
		this.awaitingPong = false;
		this.socket = undefined;
		log('[Stream]: WebSocket disconnected; reconnecting...');
		this.emit('_disconnected_');
	}

	/**
	 * Callback of when received a message from connection
	 */
	@bindThis
	private onMessage(message) {
		const { type, body } = JSON.parse(message.data);

		if (type == 'channel') {
			const id = body.id;

			let connections: (Connection | undefined)[];

			connections = this.sharedConnections.filter(c => c.id === id);

			if (connections.length === 0) {
				connections = [this.nonSharedConnections.find(c => c.id === id)];
			}

			for (const c of connections.filter(c => c != null)) {
				c!.emit(body.type, body.body);
				c!.emit('*', { type: body.type, body: body.body });
			}
		} else {
			this.emit(type, body);
			this.emit('*', { type, body });
		}
	}

	/**
	 * Send a message to connection
	 */
	@bindThis
	public send(typeOrPayload, payload?) {
		const data = payload === undefined ? typeOrPayload : {
			type: typeOrPayload,
			body: payload
		};

		// まだ接続が確立されていなかったらバッファリングして次に接続した時に送信する
		if (this.state != 'connected') {
			this.buffer.push(data);
			return;
		}

		this.stream.send(JSON.stringify(data));
	}

	/**
	 * Close this connection
	 */
	@bindThis
	public close() {
		clearInterval(this.heartbeatTimer);
		this.stream.removeEventListener('open', this.onOpen);
		this.stream.removeEventListener('close', this.onClose);
		this.stream.removeEventListener('message', this.onMessage);
		this.stream.close();
	}
}

class Pool {
	public channel: string;
	public id: string;
	protected stream: Stream;
	private users = 0;
	private disposeTimerId: any;
	private isConnected = false;

	constructor(stream: Stream, channel: string) {
		this.channel = channel;
		this.stream = stream;

		this.id = Math.random().toString();
	}

	@bindThis
	public inc() {
		if (this.users === 0 && !this.isConnected) {
			this.connect();
		}

		this.users++;

		// タイマー解除
		if (this.disposeTimerId) {
			clearTimeout(this.disposeTimerId);
			this.disposeTimerId = null;
		}
	}

	@bindThis
	public dec() {
		this.users--;

		// そのコネクションの利用者が誰もいなくなったら
		if (this.users === 0) {
			// また直ぐに再利用される可能性があるので、一定時間待ち、
			// 新たな利用者が現れなければコネクションを切断する
			this.disposeTimerId = setTimeout(() => {
				this.disconnect();
			}, 3000);
		}
	}

	@bindThis
	public connect() {
		this.isConnected = true;
		this.stream.send('connect', {
			channel: this.channel,
			id: this.id
		});
	}

	@bindThis
	private disconnect() {
		this.isConnected = false;
		this.disposeTimerId = null;
		this.stream.send('disconnect', { id: this.id });
	}
}

abstract class Connection extends EventEmitter {
	public channel: string;
	protected stream: Stream;
	public abstract id: string;

	constructor(stream: Stream, channel: string) {
		super();

		this.stream = stream;
		this.channel = channel;
	}

	@bindThis
	public send(id: string, typeOrPayload, payload?) {
		const type = payload === undefined ? typeOrPayload.type : typeOrPayload;
		const body = payload === undefined ? typeOrPayload.body : payload;

		this.stream.send('ch', {
			id: id,
			type: type,
			body: body
		});
	}

	public abstract dispose(): void;
}

class SharedConnection extends Connection {
	private pool: Pool;

	public get id(): string {
		return this.pool.id;
	}

	constructor(stream: Stream, channel: string, pool: Pool) {
		super(stream, channel);

		this.pool = pool;
		this.pool.inc();
	}

	@bindThis
	public send(typeOrPayload, payload?) {
		super.send(this.pool.id, typeOrPayload, payload);
	}

	@bindThis
	public dispose() {
		this.pool.dec();
		this.removeAllListeners();
		this.stream.removeSharedConnection(this);
	}
}

class NonSharedConnection extends Connection {
	public id: string;
	protected params: any;

	constructor(stream: Stream, channel: string, params?: any) {
		super(stream, channel);

		this.params = params;
		this.id = Math.random().toString();

		this.connect();
	}

	@bindThis
	public connect() {
		this.stream.send('connect', {
			channel: this.channel,
			id: this.id,
			params: this.params
		});
	}

	@bindThis
	public send(typeOrPayload, payload?) {
		super.send(this.id, typeOrPayload, payload);
	}

	@bindThis
	public dispose() {
		this.removeAllListeners();
		this.stream.send('disconnect', { id: this.id });
		this.stream.disconnectToChannel(this);
	}
}
