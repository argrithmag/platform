package kv

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/influxdata/platform"
)

// TODO: this is just a proof of concept

var (
	userBucket = []byte("usersv1")
	userIndex  = []byte("userindexv1")
)

var _ platform.UserService = (*UserService)(nil)

type UserService struct {
	kv          Store
	idGenerator platform.IDGenerator
}

func NewUserService(kv Store, idGen platform.IDGenerator) *UserService {
	return &UserService{
		kv:          kv,
		idGenerator: idGen,
	}
}

func (c *UserService) Initialize() error {
	return c.kv.Update(func(tx Tx) error {
		if _, err := tx.Bucket([]byte(userBucket)); err != nil {
			return err
		}
		if _, err := tx.Bucket([]byte(userIndex)); err != nil {
			return err
		}
		return nil
	})
}

// FindUserByID retrieves a user by id.
func (c *UserService) FindUserByID(ctx context.Context, id platform.ID) (*platform.User, error) {
	var u *platform.User

	err := c.kv.View(func(tx Tx) error {
		usr, err := c.findUserByID(ctx, tx, id)
		if err != nil {
			return err
		}
		u = usr
		return nil
	})

	if err != nil {
		return nil, err
	}

	return u, nil
}

func (c *UserService) findUserByID(ctx context.Context, tx Tx, id platform.ID) (*platform.User, error) {
	encodedID, err := id.Encode()
	if err != nil {
		return nil, err
	}

	b, err := tx.Bucket(userBucket)
	if err != nil {
		return nil, err
	}

	v, err := b.Get(encodedID)
	if err == ErrKeyNotFound {
		return nil, &platform.Error{
			Code: platform.ENotFound,
			Msg:  "user not found",
		}
	}
	if err != nil {
		return nil, err
	}

	var u platform.User
	if err := json.Unmarshal(v, &u); err != nil {
		return nil, err
	}

	return &u, nil
}

// FindUserByName returns a user by name for a particular user.
func (c *UserService) FindUserByName(ctx context.Context, n string) (*platform.User, error) {
	var u *platform.User

	err := c.kv.View(func(tx Tx) error {
		usr, err := c.findUserByName(ctx, tx, n)
		if err != nil {
			return err
		}
		u = usr
		return nil
	})

	return u, err
}

func (c *UserService) findUserByName(ctx context.Context, tx Tx, n string) (*platform.User, error) {
	b, err := tx.Bucket(userIndex)
	if err != nil {
		return nil, err
	}
	uid, err := b.Get(userIndexKey(n))
	if err == ErrKeyNotFound {
		return nil, &platform.Error{
			Code: platform.ENotFound,
			Msg:  "user not found",
		}
	}
	if err != nil {
		return nil, err
	}

	var id platform.ID
	if err := id.Decode(uid); err != nil {
		return nil, err
	}
	return c.findUserByID(ctx, tx, id)
}

// FindUser retrives a user using an arbitrary user filter.
// Filters using ID, or Name should be efficient.
// Other filters will do a linear scan across users until it finds a match.
func (c *UserService) FindUser(ctx context.Context, filter platform.UserFilter) (*platform.User, error) {
	if filter.ID != nil {
		return c.FindUserByID(ctx, *filter.ID)
	}

	if filter.Name != nil {
		return c.FindUserByName(ctx, *filter.Name)
	}

	filterFn := filterUsersFn(filter)

	var u *platform.User
	err := c.kv.View(func(tx Tx) error {
		return forEachUser(ctx, tx, func(usr *platform.User) bool {
			if filterFn(usr) {
				u = usr
				return false
			}
			return true
		})
	})

	if err != nil {
		return nil, err
	}

	if u == nil {
		return nil, fmt.Errorf("user not found")
	}

	return u, nil
}

func filterUsersFn(filter platform.UserFilter) func(u *platform.User) bool {
	if filter.ID != nil {
		return func(u *platform.User) bool {
			return u.ID.Valid() && u.ID == *filter.ID
		}
	}

	if filter.Name != nil {
		return func(u *platform.User) bool {
			return u.Name == *filter.Name
		}
	}

	return func(u *platform.User) bool { return true }
}

// FindUsers retrives all users that match an arbitrary user filter.
// Filters using ID, or Name should be efficient.
// Other filters will do a linear scan across all users searching for a match.
func (c *UserService) FindUsers(ctx context.Context, filter platform.UserFilter, opt ...platform.FindOptions) ([]*platform.User, int, error) {
	if filter.ID != nil {
		u, err := c.FindUserByID(ctx, *filter.ID)
		if err != nil {
			return nil, 0, err
		}

		return []*platform.User{u}, 1, nil
	}

	if filter.Name != nil {
		u, err := c.FindUserByName(ctx, *filter.Name)
		if err != nil {
			return nil, 0, err
		}

		return []*platform.User{u}, 1, nil
	}

	us := []*platform.User{}
	filterFn := filterUsersFn(filter)
	err := c.kv.View(func(tx Tx) error {
		return forEachUser(ctx, tx, func(u *platform.User) bool {
			if filterFn(u) {
				us = append(us, u)
			}
			return true
		})
	})

	if err != nil {
		return nil, 0, err
	}

	return us, len(us), nil
}

// CreateUser creates a platform user and sets b.ID.
func (c *UserService) CreateUser(ctx context.Context, u *platform.User) error {
	return c.kv.Update(func(tx Tx) error {
		unique := c.uniqueUserName(ctx, tx, u)

		if !unique {
			// TODO: make standard error
			return fmt.Errorf("user with name %s already exists", u.Name)
		}

		u.ID = c.idGenerator.ID()

		return c.putUser(ctx, tx, u)
	})
}

// PutUser will put a user without setting an ID.
func (c *UserService) PutUser(ctx context.Context, u *platform.User) error {
	return c.kv.Update(func(tx Tx) error {
		return c.putUser(ctx, tx, u)
	})
}

func (c *UserService) putUser(ctx context.Context, tx Tx, u *platform.User) error {
	v, err := json.Marshal(u)
	if err != nil {
		return err
	}
	encodedID, err := u.ID.Encode()
	if err != nil {
		return err
	}

	idx, err := tx.Bucket(userIndex)
	if err != nil {
		return err
	}

	if err := idx.Put(userIndexKey(u.Name), encodedID); err != nil {
		return err
	}

	b, err := tx.Bucket(userBucket)
	if err != nil {
		return err
	}

	return b.Put(encodedID, v)
}

func userIndexKey(n string) []byte {
	return []byte(n)
}

// forEachUser will iterate through all users while fn returns true.
func forEachUser(ctx context.Context, tx Tx, fn func(*platform.User) bool) error {
	b, err := tx.Bucket(userBucket)
	if err != nil {
		return err
	}

	cur, err := b.Cursor()
	if err != nil {
		return err
	}

	for k, v := cur.First(); k != nil; k, v = cur.Next() {
		u := &platform.User{}
		if err := json.Unmarshal(v, u); err != nil {
			return err
		}
		if !fn(u) {
			break
		}
	}

	return nil
}

func (c *UserService) uniqueUserName(ctx context.Context, tx Tx, u *platform.User) bool {
	idx, err := tx.Bucket(userIndex)
	if err != nil {
		return false
	}

	if _, err := idx.Get(userIndexKey(u.Name)); err == ErrKeyNotFound {
		return true
	}
	return false
}

// UpdateUser updates a user according the parameters set on upd.
func (c *UserService) UpdateUser(ctx context.Context, id platform.ID, upd platform.UserUpdate) (*platform.User, error) {
	var u *platform.User
	err := c.kv.Update(func(tx Tx) error {
		usr, err := c.updateUser(ctx, tx, id, upd)
		if err != nil {
			return err
		}
		u = usr
		return nil
	})

	return u, err
}

func (c *UserService) updateUser(ctx context.Context, tx Tx, id platform.ID, upd platform.UserUpdate) (*platform.User, error) {
	u, err := c.findUserByID(ctx, tx, id)
	if err != nil {
		return nil, err
	}

	if upd.Name != nil {
		// Users are indexed by name and so the user index must be pruned
		// when name is modified.
		idx, err := tx.Bucket(userIndex)
		if err != nil {
			return nil, err
		}

		if err := idx.Delete(userIndexKey(u.Name)); err != nil {
			return nil, err
		}
		u.Name = *upd.Name
	}

	if err := c.putUser(ctx, tx, u); err != nil {
		return nil, err
	}

	return u, nil
}

// DeleteUser deletes a user and prunes it from the index.
func (c *UserService) DeleteUser(ctx context.Context, id platform.ID) error {
	return c.kv.Update(func(tx Tx) error {
		return c.deleteUser(ctx, tx, id)
	})
}

func (c *UserService) deleteUser(ctx context.Context, tx Tx, id platform.ID) error {
	u, err := c.findUserByID(ctx, tx, id)
	if err != nil {
		return err
	}
	encodedID, err := id.Encode()
	if err != nil {
		return err
	}

	idx, err := tx.Bucket(userIndex)
	if err != nil {
		return err
	}

	if err := idx.Delete(userIndexKey(u.Name)); err != nil {
		return err
	}

	b, err := tx.Bucket(userBucket)
	if err != nil {
		return err
	}
	if err := b.Delete(encodedID); err != nil {
		return err
	}

	return nil
}
