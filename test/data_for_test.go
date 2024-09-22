// Data model and testing data

package test

import (
	. "github.com/go-yaaf/yaaf-common/entity"
	. "github.com/go-yaaf/yaaf-common/messaging"
	"github.com/go-yaaf/yaaf-common/utils/binary"
	"math/rand"
	"time"
)

// region Heroes Test Model --------------------------------------------------------------------------------------------
type Hero struct {
	BaseEntity
	Key  int    `json:"key"`  // Key
	Name string `json:"name"` // Name
}

func (a *Hero) TABLE() string { return "hero" }
func (a *Hero) NAME() string  { return a.Name }

func (a *Hero) MarshalBinary() (data []byte, err error) {
	w := binary.NewWriter()
	w.String(a.Id).Timestamp(a.CreatedOn).Timestamp(a.UpdatedOn).Int(a.Key).String(a.Name)
	return w.GetBytes(), nil
}

func (a *Hero) UnmarshalBinary(data []byte) (e error) {
	r := binary.NewReader(data)
	if a.Id, e = r.String(); e != nil {
		return e
	}
	if a.CreatedOn, e = r.Timestamp(); e != nil {
		return e
	}
	if a.UpdatedOn, e = r.Timestamp(); e != nil {
		return e
	}
	if a.Key, e = r.Int(); e != nil {
		return e
	}
	if a.Name, e = r.String(); e != nil {
		return e
	}
	return nil
}

func NewHero() Entity {
	return &Hero{}
}

func NewHero1(id string, key int, name string) Entity {
	return &Hero{
		BaseEntity: BaseEntity{Id: id, CreatedOn: Now(), UpdatedOn: Now()},
		Key:        key,
		Name:       name,
	}
}

var list_of_heroes = []Entity{
	NewHero1("1", 1, "Ant man"),
	NewHero1("2", 2, "Aqua man"),
	NewHero1("3", 3, "Asterix"),
	NewHero1("4", 4, "Bat Girl"),
	NewHero1("5", 5, "Bat Man"),
	NewHero1("6", 6, "Bat Woman"),
	NewHero1("7", 7, "Black Canary"),
	NewHero1("8", 8, "Black Panther"),
	NewHero1("9", 9, "Captain America"),
	NewHero1("10", 10, "Captain Marvel"),
	NewHero1("11", 11, "Cat Woman"),
	NewHero1("12", 12, "Conan the Barbarian"),
	NewHero1("13", 13, "Daredevil"),
	NewHero1("14", 14, "Doctor Strange"),
	NewHero1("15", 15, "Elektra"),
	NewHero1("16", 16, "Ghost Rider"),
	NewHero1("17", 17, "Green Arrow"),
	NewHero1("18", 18, "Green Lantern"),
	NewHero1("19", 19, "Hawkeye"),
	NewHero1("20", 20, "Hellboy"),
	NewHero1("21", 21, "Iron Man"),
	NewHero1("22", 22, "Robin"),
	NewHero1("23", 23, "Spider Man"),
	NewHero1("24", 24, "Supergirl"),
	NewHero1("25", 25, "Superman"),
	NewHero1("26", 26, "Thor"),
	NewHero1("27", 27, "The Wasp"),
	NewHero1("28", 28, "Wolverine"),
	NewHero1("29", 29, "Wonder Woman"),
	NewHero1("30", 30, "X-Man"),
}

func GetRandomHero() Entity {
	ind := rand.Intn(len(list_of_heroes))
	return list_of_heroes[ind]
}

func GetRandomHeroMessage(topic string) IMessage {
	hero := GetRandomHero()
	return newHeroMessage(topic, hero.(*Hero))
}

// endregion

// region Domain message for the Test -----------------------------------------------------------------------------------

type HeroMessage struct {
	BaseMessage
	Hero *Hero `json:"hero"`
}

func (m *HeroMessage) Payload() any { return m.Hero }

func NewHeroMessage() IMessage {
	return &HeroMessage{}
}

func newHeroMessage(topic string, hero *Hero) IMessage {
	message := &HeroMessage{
		Hero: hero,
	}
	message.MsgTopic = topic
	message.MsgOpCode = int(time.Now().Unix())
	message.MsgSessionId = NanoID()
	message.MsgAddressee = hero.Name
	return message
}

// endregion
